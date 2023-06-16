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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use log::{debug, error, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventLoop, EventSender};

use crate::metrics::SchedulerMetricsCollector;
use crate::scheduler_server::timestamp_millis;
use ballista_core::serde::protobuf::TaskStatus;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::scheduler_server::event::{
    JobDataCleanupEvent, JobStateCleanupEvent, QueryStageSchedulerEvent,
    TaskStatusUpdateEvent,
};
use crate::state::event_action::{
    JobDataCleaner, JobStateCleaner, OfferReviver, TaskStatusUpdater,
};

use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    job_data_cleaner_event_loop: EventLoop<JobDataCleanupEvent>,
    job_state_cleaner_event_loop: EventLoop<JobStateCleanupEvent>,
    offer_reviver_event_loop: EventLoop<()>,
    task_status_updater_event_loop: EventLoop<TaskStatusUpdateEvent>,
    metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    event_expected_processing_duration: u64,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        event_expected_processing_duration: u64,
    ) -> Self {
        let job_data_cleaner = JobDataCleaner::new(state.clone());
        let mut job_data_cleaner_event_loop = EventLoop::new(
            "JobDataCleaner".to_string(),
            state.config.event_loop_buffer_size as usize,
            Arc::new(job_data_cleaner),
        );
        job_data_cleaner_event_loop
            .start()
            .expect("Fail to start the event loop for cleaning up job data");

        let job_state_cleaner = JobStateCleaner::new(state.clone());
        let mut job_state_cleaner_event_loop = EventLoop::new(
            "JobStateCleaner".to_string(),
            state.config.event_loop_buffer_size as usize,
            Arc::new(job_state_cleaner),
        );
        job_state_cleaner_event_loop
            .start()
            .expect("Fail to start the event loop for cleaning up job state");

        let offer_reviver = OfferReviver::new(state.clone());
        let mut offer_reviver_event_loop = EventLoop::new(
            "OfferReviver".to_string(),
            state.config.event_loop_buffer_size as usize,
            Arc::new(offer_reviver),
        );
        offer_reviver_event_loop
            .start()
            .expect("Fail to start the event loop for revive offers");

        let task_status_updater = TaskStatusUpdater::new(state.clone());
        let mut task_status_updater_event_loop = EventLoop::new(
            "TaskStatusUpdater".to_string(),
            state.config.event_loop_buffer_size as usize,
            Arc::new(task_status_updater),
        );
        task_status_updater_event_loop
            .start()
            .expect("Fail to start the event loop for task status update");

        Self {
            state,
            job_data_cleaner_event_loop,
            job_state_cleaner_event_loop,
            offer_reviver_event_loop,
            task_status_updater_event_loop,
            metrics_collector,
            event_expected_processing_duration,
        }
    }

    pub(crate) fn metrics_collector(&self) -> &dyn SchedulerMetricsCollector {
        self.metrics_collector.as_ref()
    }

    /// For cleaning up a successful job,
    /// - clean the shuffle data on executors delayed
    /// - clean the state on schedulers delayed
    async fn clean_up_successful_job(&self, job_id: String) {
        if self
            .state
            .config
            .finished_job_data_clean_up_interval_seconds
            > 0
        {
            self.send_job_data_cleanup_event(job_id.clone(), true).await;
        }
        if self
            .state
            .config
            .finished_job_state_clean_up_interval_seconds
            > 0
        {
            self.send_job_state_cleanup_event(job_id, true).await;
        }
    }

    /// For cleaning up a failed job,
    /// - clean the shuffle data on executors immediately
    /// - clean the state on schedulers delayed
    async fn clean_up_failed_job(&self, job_id: String) {
        self.send_job_data_cleanup_event(job_id.clone(), false)
            .await;
        if self
            .state
            .config
            .finished_job_state_clean_up_interval_seconds
            > 0
        {
            self.send_job_state_cleanup_event(job_id, true).await;
        }
    }

    async fn send_job_data_cleanup_event(&self, job_id: String, delayed: bool) {
        if let Ok(sender) = self.job_data_cleaner_event_loop.get_sender() {
            let event = if delayed {
                let deadline = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs()
                    + self
                        .state
                        .config
                        .finished_job_data_clean_up_interval_seconds;
                JobDataCleanupEvent::Delayed { job_id, deadline }
            } else {
                JobDataCleanupEvent::Immediate { job_id }
            };
            if let Err(e) = sender.post_event(event.clone()).await {
                warn!("Fail to send event {:?} due to {:?}", event, e);
            }
        } else {
            warn!("Fail to get event sender for JobDataCleanup");
        }
    }

    async fn send_job_state_cleanup_event(&self, job_id: String, delayed: bool) {
        if let Ok(sender) = self.job_state_cleaner_event_loop.get_sender() {
            let event = if delayed {
                let deadline = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs()
                    + self
                        .state
                        .config
                        .finished_job_state_clean_up_interval_seconds;
                JobStateCleanupEvent::Delayed { job_id, deadline }
            } else {
                JobStateCleanupEvent::Immediate { job_id }
            };
            if let Err(e) = sender.post_event(event.clone()).await {
                warn!("Fail to send event {:?} due to {:?}", event, e);
            }
        } else {
            warn!("Fail to get event sender for JobStateCleanup");
        }
    }

    async fn send_revive_offers_event(&self) {
        if let Ok(sender) = self.offer_reviver_event_loop.get_sender() {
            if let Err(e) = sender.post_event(()).await {
                error!("Fail to send revive offers event due to {:?}", e);
            }
        } else {
            error!("Fail to get event sender for OfferReviver");
        }
    }

    async fn send_task_status_updater_event(
        &self,
        executor_id: String,
        tasks_status: Vec<TaskStatus>,
        stage_event_sender: EventSender<QueryStageSchedulerEvent>,
    ) {
        debug!(
            "processing task status updates from {executor_id}: {:?}",
            tasks_status
        );

        let num_status = tasks_status.len();
        if self.state.config.is_push_staged_scheduling() {
            if let Err(e) = self
                .state
                .executor_manager
                .unbind_tasks(vec![(executor_id.clone(), num_status as u32)])
                .await
            {
                error!(
                    "Fail to unbind {} slots for executor{}: {}",
                    num_status, executor_id, e
                );
            }
        }

        if let Ok(sender) = self.task_status_updater_event_loop.get_sender() {
            if let Err(e) = sender
                .post_event(TaskStatusUpdateEvent {
                    executor_id,
                    tasks_status,
                    stage_event_sender,
                })
                .await
            {
                error!("Fail to send task status update event due to {:?}", e);
            }
        } else {
            error!("Fail to get event sender for TaskStatusUpdater");
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<QueryStageSchedulerEvent> for QueryStageScheduler<T, U>
{
    fn on_start(&self) {
        info!("Starting QueryStageScheduler");
    }

    fn on_stop(&self) {
        info!("Stopping QueryStageScheduler")
    }

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
        tx_event: &mpsc::Sender<QueryStageSchedulerEvent>,
        _rx_event: &mut mpsc::Receiver<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let mut time_recorder = None;
        if self.event_expected_processing_duration > 0 {
            time_recorder = Some((Instant::now(), event.clone()));
        };
        let tx_event = EventSender::new(tx_event.clone());
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                job_name,
                session_ctx,
                plan,
                queued_at,
            } => {
                info!("Job {} queued with name {:?}", job_id, job_name);

                if let Err(e) = self
                    .state
                    .task_manager
                    .queue_job(&job_id, &job_name, queued_at)
                    .await
                {
                    error!("Fail to queue job {} due to {:?}", job_id, e);
                    return Ok(());
                }

                let state = self.state.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(&job_id, &job_name, session_ctx, &plan, queued_at)
                        .await
                    {
                        let fail_message = format!("Error planning job {job_id}: {e:?}");
                        error!("{}", &fail_message);
                        QueryStageSchedulerEvent::JobPlanningFailed {
                            job_id,
                            fail_message,
                            queued_at,
                            failed_at: timestamp_millis(),
                        }
                    } else {
                        QueryStageSchedulerEvent::JobSubmitted {
                            job_id,
                            queued_at,
                            submitted_at: timestamp_millis(),
                        }
                    };
                    if let Err(e) = tx_event.post_event(event).await {
                        error!("Fail to send event due to {}", e);
                    }
                });
            }
            QueryStageSchedulerEvent::JobSubmitted {
                job_id,
                queued_at,
                submitted_at,
            } => {
                self.metrics_collector
                    .record_submitted(&job_id, queued_at, submitted_at);

                info!("Job {} submitted", job_id);

                if self.state.config.is_push_staged_scheduling() {
                    self.send_revive_offers_event().await;
                }
            }
            QueryStageSchedulerEvent::JobPlanningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job {} failed: {}", job_id, fail_message);
                if let Err(e) = self
                    .state
                    .task_manager
                    .fail_unscheduled_job(&job_id, fail_message)
                    .await
                {
                    error!(
                        "Fail to invoke fail_unscheduled_job for job {} due to {:?}",
                        job_id, e
                    );
                }
            }
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                self.metrics_collector
                    .record_completed(&job_id, queued_at, completed_at);

                info!("Job {} success", job_id);
                if let Err(e) = self.state.task_manager.succeed_job(&job_id).await {
                    error!(
                        "Fail to invoke succeed_job for job {} due to {:?}",
                        job_id, e
                    );
                }

                self.clean_up_successful_job(job_id).await;
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job {} running failed due to {}", job_id, fail_message);
                match self
                    .state
                    .task_manager
                    .abort_job(&job_id, fail_message)
                    .await
                {
                    Ok((running_tasks, _pending_tasks)) => {
                        if !running_tasks.is_empty() {
                            tx_event
                                .post_event(QueryStageSchedulerEvent::CancelTasks(
                                    running_tasks,
                                ))
                                .await?;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Fail to invoke abort_job for job {} due to {:?}",
                            job_id, e
                        );
                    }
                }

                self.clean_up_failed_job(job_id).await;
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                info!("Job {} Updated", job_id);
                if let Err(e) = self.state.task_manager.update_job(&job_id).await {
                    error!(
                        "Fail to invoke update_job for job {} due to {:?}",
                        job_id, e
                    );
                }
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                self.metrics_collector.record_cancelled(&job_id);

                info!("Job {} Cancelled", job_id);
                match self.state.task_manager.cancel_job(&job_id).await {
                    Ok((running_tasks, _pending_tasks)) => {
                        tx_event
                            .post_event(QueryStageSchedulerEvent::CancelTasks(
                                running_tasks,
                            ))
                            .await?;
                    }
                    Err(e) => {
                        error!(
                            "Fail to invoke cancel_job for job {} due to {:?}",
                            job_id, e
                        );
                    }
                }

                self.clean_up_failed_job(job_id).await;
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status) => {
                self.send_task_status_updater_event(executor_id, tasks_status, tx_event)
                    .await
            }
            QueryStageSchedulerEvent::ReviveOffers => {
                self.send_revive_offers_event().await;
            }
            QueryStageSchedulerEvent::ExecutorLost(executor_id, reason) => {
                self.state.remove_executor(&executor_id, reason).await;
                if self.state.config.is_push_staged_scheduling() {
                    self.send_revive_offers_event().await;
                }
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                if let Err(e) = self
                    .state
                    .executor_manager
                    .cancel_running_tasks(tasks)
                    .await
                {
                    warn!("Fail to cancel running tasks due to {:?}", e);
                }
            }
            QueryStageSchedulerEvent::JobDataClean(job_id) => {
                self.send_job_data_cleanup_event(job_id, false).await;
            }
        }
        if let Some((start, ec)) = time_recorder {
            let duration = start.elapsed();
            if duration.ge(&Duration::from_micros(
                self.event_expected_processing_duration,
            )) {
                warn!(
                    "[METRICS] {:?} event cost {:?} us!",
                    ec,
                    duration.as_micros()
                );
            }
        }
        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}
