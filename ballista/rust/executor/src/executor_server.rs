// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use log::{debug, error, info, warn};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::executor_grpc_server::{
    ExecutorGrpc, ExecutorGrpcServer,
};
use ballista_core::serde::protobuf::executor_registration::OptionalHost;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{
    HeartBeatParams, LaunchMultiTaskParams, LaunchTaskParams, LaunchTaskResult,
    MultiTaskDefinition, RegisterExecutorParams, StopExecutorParams, StopExecutorResult,
    TaskDefinition, TaskStatus, UpdateTaskStatusParams,
};
use ballista_core::serde::scheduler;
use ballista_core::serde::scheduler::{
    PartitionId, PartitionIds, SharedTaskContext, SimpleFunctionRegistry,
};
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};
use datafusion::execution::context::TaskContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use tokio::sync::mpsc::error::TryRecvError;

use crate::as_task_status;
use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;

pub async fn startup<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
) {
    // TODO make the buffer size configurable
    let (tx_task, rx_task) = mpsc::channel::<(PartitionId, SharedTaskContext)>(1000);
    let (tx_task_status, rx_task_status) = mpsc::channel::<TaskStatus>(1000);

    let executor_server = ExecutorServer::new(
        scheduler.clone(),
        executor.clone(),
        ExecutorEnv {
            tx_task,
            tx_task_status,
        },
        codec,
    );

    // 1. Start executor grpc service
    {
        let executor_meta = executor.metadata.clone();
        let addr = format!(
            "{}:{}",
            executor_meta
                .optional_host
                .map(|h| match h {
                    OptionalHost::Host(host) => host,
                })
                .unwrap_or_else(|| String::from("127.0.0.1")),
            executor_meta.grpc_port
        );
        let addr = addr.parse().unwrap();
        info!("Setup executor grpc service for {:?}", addr);

        let server = ExecutorGrpcServer::new(executor_server.clone());
        let grpc_server_future = Server::builder().add_service(server).serve(addr);
        tokio::spawn(async move { grpc_server_future.await });
    }

    let executor_server = Arc::new(executor_server);

    // 2. Do executor registration
    match register_executor(&mut scheduler, executor.clone()).await {
        Ok(_) => {
            info!("Executor registration succeed");
        }
        Err(error) => {
            panic!("Executor registration failed due to: {}", error);
        }
    };

    // 3. Start Heartbeater
    {
        let heartbeater = Heartbeater::new(executor_server.clone());
        heartbeater.start().await;
    }

    // 4. Start TaskRunnerPool
    {
        let task_runner_pool = TaskRunnerPool::new(executor_server.clone());
        task_runner_pool.start(rx_task, rx_task_status).await;
    }
}

#[allow(clippy::clone_on_copy)]
async fn register_executor(
    scheduler: &mut SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
) -> Result<(), BallistaError> {
    let result = scheduler
        .register_executor(RegisterExecutorParams {
            metadata: Some(executor.metadata.clone()),
        })
        .await?;
    if result.into_inner().success {
        Ok(())
    } else {
        Err(BallistaError::General(
            "Executor registration failed!!!".to_owned(),
        ))
    }
}

#[derive(Clone)]
pub struct ExecutorServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    _start_time: u128,
    executor: Arc<Executor>,
    scheduler: SchedulerGrpcClient<Channel>,
    executor_env: ExecutorEnv,
    codec: BallistaCodec<T, U>,
}

#[derive(Clone)]
struct ExecutorEnv {
    tx_task: mpsc::Sender<(PartitionId, SharedTaskContext)>,
    tx_task_status: mpsc::Sender<TaskStatus>,
}

unsafe impl Sync for ExecutorEnv {}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorServer<T, U> {
    fn new(
        scheduler: SchedulerGrpcClient<Channel>,
        executor: Arc<Executor>,
        executor_env: ExecutorEnv,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            _start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            executor,
            scheduler,
            executor_env,
            codec,
        }
    }

    async fn heartbeat(&self) {
        match self
            .scheduler
            .clone()
            .heart_beat_from_executor(HeartBeatParams {
                executor_id: self.executor.metadata.id.clone(),
                state: Some(self.get_executor_state().into()),
                metadata: Some(self.executor.metadata.clone()),
            })
            .await
        {
            // TODO heartbeat result
            Ok(_heartbeat_result) => {
                // ignore the result
            }
            Err(e) => {
                // just log the warn and not panic
                warn!("Send the heartbeat and meet the error: {:?}", e);
            }
        }
    }

    async fn run_task(
        &self,
        task: (PartitionId, SharedTaskContext),
    ) -> Result<(), BallistaError> {
        let task_id = task.0;
        let task_id_log = format!(
            "{}/{}/{}",
            task_id.job_id, task_id.stage_id, task_id.partition_id
        );
        info!("Start to run task {}", task_id_log);

        let task_context = Arc::new(TaskContext::new(
            task_id_log.clone(),
            task.1.session_id,
            task.1.props,
            task.1.function_registry.scalar_functions,
            task.1.function_registry.aggregate_functions,
            self.executor.runtime.clone(),
        ));

        let execution_result = self
            .executor
            .execute_shuffle_write(
                task_id.job_id.clone(),
                task_id.stage_id as usize,
                task_id.partition_id as usize,
                task.1.plan,
                task_context,
                task.1.output_partitioning,
            )
            .await;
        info!("Done with task {}", task_id_log);
        debug!("Statistics: {:?}", execution_result);

        let executor_id = &self.executor.metadata.id;
        let task_status =
            as_task_status(execution_result, executor_id.clone(), task_id.into());

        let task_status_sender = self.executor_env.tx_task_status.clone();
        task_status_sender.send(task_status).await.unwrap();

        Ok(())
    }

    // TODO with real state
    fn get_executor_state(&self) -> scheduler::ExecutorState {
        scheduler::ExecutorState {
            available_memory_size: u64::MAX,
        }
    }
}

struct Heartbeater<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> Heartbeater<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    async fn start(&self) {
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting heartbeater to send heartbeat the scheduler periodically");
            loop {
                executor_server.heartbeat().await;
                tokio::time::sleep(Duration::from_millis(60000)).await;
            }
        });
    }
}

struct TaskRunnerPool<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskRunnerPool<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    async fn start(
        &self,
        mut rx_task: mpsc::Receiver<(PartitionId, SharedTaskContext)>,
        mut rx_task_status: mpsc::Receiver<TaskStatus>,
    ) {
        // loop for task status reporting
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting the task status reporter");
            loop {
                let mut tasks_status = vec![];
                // First try to fetch task status from the channel in blocking mode
                if let Some(task_status) = rx_task_status.recv().await {
                    tasks_status.push(task_status);
                } else {
                    info!("Channel is closed and will exit the loop");
                    return;
                }
                // Then try to fetch by non-blocking mode to fetch as much finished tasks as possible
                loop {
                    match rx_task_status.try_recv() {
                        Ok(task_status) => {
                            tasks_status.push(task_status);
                        }
                        Err(TryRecvError::Empty) => {
                            info!(
                                "Fetched {} tasks status to report",
                                tasks_status.len()
                            );
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            info!("Channel is closed and will exit the loop");
                            return;
                        }
                    }
                }

                if let Err(e) = executor_server
                    .scheduler
                    .clone()
                    .update_task_status(UpdateTaskStatusParams {
                        executor_id: executor_server.executor.metadata.id.clone(),
                        task_status: tasks_status.clone(),
                    })
                    .await
                {
                    error!("Fail to update tasks {:?} due to {:?}", tasks_status, e);
                }
            }
        });

        // loop for task fetching and running
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting the task runner pool");
            // Use a dedicated executor for CPU bound tasks so that the main tokio
            // executor can still answer requests even when under load
            let dedicated_executor = DedicatedExecutor::new(
                "task_runner",
                executor_server.executor.concurrent_tasks,
            );
            loop {
                if let Some(task) = rx_task.recv().await {
                    let task_id_log = format!(
                        "{}/{}/{}",
                        task.0.job_id, task.0.stage_id, task.0.partition_id
                    );
                    info!("Received task {:?}", &task_id_log);

                    let server = executor_server.clone();
                    dedicated_executor.spawn(async move {
                        server.run_task(task).await.unwrap_or_else(|e| {
                            error!(
                                "Fail to run the task {:?} due to {:?}",
                                task_id_log, e
                            );
                        });
                    });
                } else {
                    info!("Channel is closed and will exit the loop");
                    return;
                }
            }
        });
    }
}

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorGrpc
    for ExecutorServer<T, U>
{
    async fn launch_task(
        &self,
        request: Request<LaunchTaskParams>,
    ) -> Result<Response<LaunchTaskResult>, Status> {
        let tasks = request.into_inner().task;
        let task_sender = self.executor_env.tx_task.clone();
        for task in tasks {
            let task_id = task.task_id.clone().unwrap().into();
            let function_registry = get_function_registry(&task, self.executor.as_ref());
            let task_ctx: SharedTaskContext = (
                task,
                function_registry,
                self.executor.runtime.as_ref(),
                &self.codec,
            )
                .try_into()
                .map_err(|e| {
                    tonic::Status::internal(format!(
                        "Could not deserialize task definition: {}",
                        e
                    ))
                })?;
            task_sender.send((task_id, task_ctx)).await.unwrap();
        }
        Ok(Response::new(LaunchTaskResult { success: true }))
    }

    /// by this interface, it can reduce the deserialization cost for multiple tasks
    /// belong to the same job stage running on the same one executor
    async fn launch_multi_task(
        &self,
        request: Request<LaunchMultiTaskParams>,
    ) -> Result<Response<LaunchTaskResult>, Status> {
        let multi_tasks = request.into_inner().multi_tasks;
        let task_sender = self.executor_env.tx_task.clone();
        for multi_task in multi_tasks {
            let task_ids: PartitionIds = multi_task.task_ids.clone().unwrap().into();
            let function_registry =
                get_function_registry2(&multi_task, self.executor.as_ref());
            for partition_id in task_ids.partition_ids.into_iter() {
                // Currently we have to deserialize for each task to avoid metrics in the ExecutionPlan
                // to influence each other. Later we may optimize it.
                let task_ctx: SharedTaskContext = (
                    multi_task.clone(),
                    function_registry.clone(),
                    self.executor.runtime.as_ref(),
                    &self.codec,
                )
                    .try_into()
                    .map_err(|e| {
                        tonic::Status::internal(format!(
                            "Could not deserialize task definition: {}",
                            e
                        ))
                    })?;
                task_sender
                    .send((
                        PartitionId::new(
                            &task_ids.job_id,
                            task_ids.stage_id,
                            partition_id,
                        ),
                        task_ctx,
                    ))
                    .await
                    .unwrap();
            }
        }
        Ok(Response::new(LaunchTaskResult { success: true }))
    }

    async fn stop_executor(
        &self,
        _request: Request<StopExecutorParams>,
    ) -> Result<Response<StopExecutorResult>, Status> {
        todo!()
    }
}

/// Get the function registry for an executor task with combining both UDFs and UDFSs
fn get_function_registry(
    _task: &TaskDefinition,
    executor: &Executor,
) -> SimpleFunctionRegistry {
    // TODO combine the functions from Executor's functions and TaskDefintion's function resources
    SimpleFunctionRegistry {
        scalar_functions: executor.scalar_functions.clone(),
        aggregate_functions: executor.aggregate_functions.clone(),
    }
}

/// Get the function registry for a set of executor tasks with combining both UDFs and UDFSs
fn get_function_registry2(
    _multi_task: &MultiTaskDefinition,
    executor: &Executor,
) -> SimpleFunctionRegistry {
    // TODO combine the functions from Executor's functions and TaskDefintion's function resources
    SimpleFunctionRegistry {
        scalar_functions: executor.scalar_functions.clone(),
        aggregate_functions: executor.aggregate_functions.clone(),
    }
}
