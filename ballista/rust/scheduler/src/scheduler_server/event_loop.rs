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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, error, warn};

use crate::scheduler_server::event::SchedulerServerEvent;
use crate::scheduler_server::ExecutorsClient;
use crate::state::task_scheduler::TaskScheduler;
use crate::state::SchedulerState;
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventAction;
use ballista_core::serde::protobuf::{LaunchMultiTaskParams, MultiTaskDefinition};
use ballista_core::serde::scheduler::ExecutorDataChange;
use ballista_core::serde::AsExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;

pub(crate) struct SchedulerServerEventAction<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    executors_client: ExecutorsClient,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    SchedulerServerEventAction<T, U>
{
    pub fn new(
        state: Arc<SchedulerState<T, U>>,
        executors_client: ExecutorsClient,
    ) -> Self {
        Self {
            state,
            executors_client,
        }
    }

    #[allow(unused_variables)]
    async fn offer_resources(&self, n: u32) -> Result<Option<SchedulerServerEvent>> {
        let mut executors_data =
            self.state.executor_manager.get_available_executors_data();
        // In case of there's no enough resources, reschedule the tasks of the job
        if executors_data.is_empty() {
            // TODO Maybe it's better to use an exclusive runtime for this kind task scheduling
            warn!("Not enough available executors for task running");
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(Some(SchedulerServerEvent::ReviveOffers(1)));
        }

        let (tasks_assigment, num_tasks) = self
            .state
            .fetch_schedulable_tasks(&mut executors_data, n)
            .await?;

        #[cfg(not(test))]
        if num_tasks > 0 {
            let executors: Vec<String> = executors_data
                .iter()
                .map(|executor_data| executor_data.0.to_owned())
                .collect();
            self.launch_tasks(&executors, tasks_assigment).await?;
        }

        Ok(None)
    }

    #[allow(dead_code)]
    async fn launch_tasks(
        &self,
        executors: &[String],
        tasks_assigment: Vec<Vec<MultiTaskDefinition>>,
    ) -> Result<()> {
        for (idx_executor, tasks) in tasks_assigment.into_iter().enumerate() {
            if !tasks.is_empty() {
                let executor_id = &executors[idx_executor];
                debug!(
                    "Start to launch tasks {:?} to executor {:?}",
                    tasks
                        .iter()
                        .map(|task| {
                            if let Some(task_ids) = task.task_ids.as_ref() {
                                format!(
                                    "{}/{}/{:?}",
                                    task_ids.job_id,
                                    task_ids.stage_id,
                                    task_ids.partition_ids
                                )
                            } else {
                                "".to_string()
                            }
                        })
                        .collect::<Vec<String>>(),
                    executor_id
                );
                let mut client = {
                    let clients = self.executors_client.read().await;
                    clients.get(executor_id).unwrap().clone()
                };
                let executor_manager = self.state.executor_manager.clone();
                let task_num: usize = tasks
                    .iter()
                    .map(|task| task.task_ids.as_ref().unwrap().partition_ids.len())
                    .sum();
                let data_change = ExecutorDataChange {
                    executor_id: executor_id.to_owned(),
                    task_slots: 0 - task_num as i32,
                };
                executor_manager.update_executor_data(&data_change);

                let sampling_num = if task_num >= 5 { 5 } else { task_num };
                let mut task_log_ids = Vec::with_capacity(sampling_num);
                for task in tasks.iter() {
                    if let Some(task_ids) = &task.task_ids {
                        for task_id in &task_ids.partition_ids {
                            task_log_ids.push(format!(
                                "{}/{}/{}",
                                task_ids.job_id, task_ids.stage_id, task_id
                            ));
                            if task_log_ids.len() >= sampling_num {
                                break;
                            }
                        }
                    }
                    if task_log_ids.len() >= sampling_num {
                        break;
                    }
                }
                debug!(
                    "Start to launch tasks {:?} for executor {}",
                    task_log_ids, executor_id
                );
                tokio::spawn(async move {
                    if let Err(e) = client
                        .launch_multi_task(LaunchMultiTaskParams { multi_tasks: tasks })
                        .await
                    {
                        // TODO deal with launching task failure case
                        error!(
                            "Fail to launch tasks for {} due to {:?}",
                            data_change.executor_id, e
                        );
                    } else {
                        debug!(
                            "Finished launching tasks {:?} for executor {}",
                            task_log_ids, data_change.executor_id
                        );
                    }
                });
            } else {
                // Since the task assignment policy is round robin,
                // if find tasks for one executor is empty, just break fast
                break;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<SchedulerServerEvent> for SchedulerServerEventAction<T, U>
{
    // TODO
    fn on_start(&self) {}

    // TODO
    fn on_stop(&self) {}

    async fn on_receive(
        &self,
        event: SchedulerServerEvent,
    ) -> Result<Option<SchedulerServerEvent>> {
        match event {
            SchedulerServerEvent::ReviveOffers(n) => self.offer_resources(n).await,
        }
    }

    // TODO
    fn on_error(&self, _error: BallistaError) {}
}
