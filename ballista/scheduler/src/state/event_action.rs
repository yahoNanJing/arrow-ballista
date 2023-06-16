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

//! Defines a few EventActions for cluster legacy data clean up

use async_trait::async_trait;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop;

use crate::scheduler_server::event::{
    JobDataCleanupEvent, JobStateCleanupEvent, QueryStageSchedulerEvent,
    TaskStatusUpdateEvent,
};
use crate::state::SchedulerState;

/// EventAction for cleaning up job shuffle data on executors
pub struct JobDataCleaner<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<SchedulerState<T, U>>,
    batch_size: usize,
    sleep_ts: u64,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> JobDataCleaner<T, U> {
    pub fn new(state: Arc<SchedulerState<T, U>>) -> Self {
        Self::new_with_batch_size(state, 32)
    }

    pub fn new_with_batch_size(
        state: Arc<SchedulerState<T, U>>,
        batch_size: usize,
    ) -> Self {
        Self {
            state,
            batch_size,
            sleep_ts: 30,
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    event_loop::EventAction<JobDataCleanupEvent> for JobDataCleaner<T, U>
{
    fn on_start(&self) {
        info!("Starting JobDataCleaner");
    }

    fn on_stop(&self) {
        info!("Stopping JobDataCleaner");
    }

    async fn on_receive(
        &self,
        event: JobDataCleanupEvent,
        tx_event: &Sender<JobDataCleanupEvent>,
        rx_event: &mut Receiver<JobDataCleanupEvent>,
    ) -> Result<()> {
        let mut events = vec![event];
        // Try to fetch events by non-blocking mode as many as possible
        loop {
            match rx_event.try_recv() {
                Ok(event) => {
                    events.push(event);
                }
                Err(TryRecvError::Empty) => {
                    info!("{} clean job data events fetched", events.len());
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Channel is closed and will exit the loop");
                    return Ok(());
                }
            }
            if events.len() >= self.batch_size {
                info!(
                    "{} clean job data events as a full batch drained",
                    events.len()
                );
                break;
            }
        }

        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut jobs_to_clean = HashSet::new();
        let mut jobs_delayed_to_clean = HashMap::new();
        let mut min_deadline = 0;
        for event in events {
            match event {
                JobDataCleanupEvent::Immediate { job_id } => {
                    jobs_to_clean.insert(job_id);
                }
                JobDataCleanupEvent::Delayed { job_id, deadline } => {
                    if deadline <= now_epoch_ts {
                        jobs_to_clean.insert(job_id);
                    } else {
                        if min_deadline == 0 || deadline < min_deadline {
                            min_deadline = deadline;
                        }
                        let old_deadline = jobs_delayed_to_clean
                            .entry(job_id.clone())
                            .or_insert_with(|| deadline);
                        // Set with smallest deadline
                        if *old_deadline > deadline {
                            *old_deadline = deadline;
                        }
                    }
                }
            }
        }

        if !jobs_to_clean.is_empty() {
            let state = self.state.clone();
            tokio::spawn(async move {
                state
                    .executor_manager
                    .clean_up_job_data(Vec::from_iter(jobs_to_clean.into_iter()))
                    .await;
            });
        }

        if !jobs_delayed_to_clean.is_empty() {
            let tx_event = tx_event.clone();
            // At least sleep [`sleep_ts`] seconds
            if min_deadline < now_epoch_ts + self.sleep_ts {
                min_deadline = now_epoch_ts + self.sleep_ts;
            }
            tokio::spawn(async move {
                assert!(min_deadline > now_epoch_ts);
                tokio::time::sleep(Duration::from_secs(min_deadline - now_epoch_ts))
                    .await;
                for (job_id, deadline) in jobs_delayed_to_clean {
                    if tx_event
                        .send(JobDataCleanupEvent::Delayed {
                            job_id: job_id.clone(),
                            deadline,
                        })
                        .await
                        .is_err()
                    {
                        error!("Fail to send JobDataCleanupEvent({job_id}, {deadline}) to the channel");
                    }
                }
            });
        }

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by JobDataCleaner: {:?}", error);
    }
}

/// EventAction for cleaning up job state on schedulers
pub struct JobStateCleaner<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<SchedulerState<T, U>>,
    batch_size: usize,
    sleep_ts: u64,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> JobStateCleaner<T, U> {
    pub fn new(state: Arc<SchedulerState<T, U>>) -> Self {
        Self::new_with_batch_size(state, 128)
    }

    pub fn new_with_batch_size(
        state: Arc<SchedulerState<T, U>>,
        batch_size: usize,
    ) -> Self {
        Self {
            state,
            batch_size,
            sleep_ts: 5,
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    event_loop::EventAction<JobStateCleanupEvent> for JobStateCleaner<T, U>
{
    fn on_start(&self) {
        info!("Starting JobStateCleaner");
    }

    fn on_stop(&self) {
        info!("Stopping JobStateCleaner");
    }

    async fn on_receive(
        &self,
        event: JobStateCleanupEvent,
        tx_event: &Sender<JobStateCleanupEvent>,
        rx_event: &mut Receiver<JobStateCleanupEvent>,
    ) -> Result<()> {
        let mut events = vec![event];
        // Try to fetch events by non-blocking mode as many as possible
        loop {
            match rx_event.try_recv() {
                Ok(event) => {
                    events.push(event);
                }
                Err(TryRecvError::Empty) => {
                    info!("{} clean job state events fetched", events.len());
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Channel is closed and will exit the loop");
                    return Ok(());
                }
            }
            if events.len() >= self.batch_size {
                info!(
                    "{} clean job state events as a full batch drained",
                    events.len()
                );
                break;
            }
        }

        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut jobs_to_clean = HashSet::new();
        let mut jobs_delayed_to_clean = HashMap::new();
        let mut min_deadline = 0;
        for event in events {
            match event {
                JobStateCleanupEvent::Immediate { job_id } => {
                    jobs_to_clean.insert(job_id);
                }
                JobStateCleanupEvent::Delayed { job_id, deadline } => {
                    if deadline <= now_epoch_ts {
                        jobs_to_clean.insert(job_id);
                    } else {
                        if min_deadline == 0 || deadline < min_deadline {
                            min_deadline = deadline;
                        }
                        let old_deadline = jobs_delayed_to_clean
                            .entry(job_id.clone())
                            .or_insert_with(|| deadline);
                        // Set with smallest deadline
                        if *old_deadline > deadline {
                            *old_deadline = deadline;
                        }
                    }
                }
            }
        }

        if !jobs_to_clean.is_empty() {
            self.state
                .task_manager
                .remove_jobs(Vec::from_iter(jobs_to_clean.into_iter()))
                .await;
        }

        if !jobs_delayed_to_clean.is_empty() {
            let tx_event = tx_event.clone();
            // At least sleep [`sleep_ts`] seconds
            if min_deadline < now_epoch_ts + self.sleep_ts {
                min_deadline = now_epoch_ts + self.sleep_ts;
            }
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(min_deadline - now_epoch_ts))
                    .await;
                for (job_id, deadline) in jobs_delayed_to_clean {
                    if tx_event
                        .send(JobStateCleanupEvent::Delayed {
                            job_id: job_id.clone(),
                            deadline,
                        })
                        .await
                        .is_err()
                    {
                        error!("Fail to send JobStateCleanupEvent({job_id}, {deadline}) to the channel");
                    }
                }
            });
        }

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by JobStateCleaner: {:?}", error);
    }
}

/// EventAction for revive offers
pub struct OfferReviver<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<SchedulerState<T, U>>,
    batch_size: usize,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> OfferReviver<T, U> {
    pub fn new(state: Arc<SchedulerState<T, U>>) -> Self {
        Self::new_with_batch_size(state, 256)
    }

    pub fn new_with_batch_size(
        state: Arc<SchedulerState<T, U>>,
        batch_size: usize,
    ) -> Self {
        Self { state, batch_size }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> event_loop::EventAction<()>
    for OfferReviver<T, U>
{
    fn on_start(&self) {
        info!("Starting OfferReviver");
    }

    fn on_stop(&self) {
        info!("Stopping OfferReviver");
    }

    async fn on_receive(
        &self,
        _event: (),
        tx_event: &Sender<()>,
        rx_event: &mut Receiver<()>,
    ) -> Result<()> {
        let mut num_events = 1usize;
        // Try to fetch events by non-blocking mode as many as possible
        loop {
            match rx_event.try_recv() {
                Ok(_) => {
                    num_events += 1;
                }
                Err(TryRecvError::Empty) => {
                    info!("{} revive offers events fetched", num_events);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Channel is closed and will exit the loop");
                    return Ok(());
                }
            }

            if num_events >= self.batch_size {
                info!(
                    "{} revive offers events as a full batch drained",
                    num_events
                );
                break;
            }
        }

        self.state.revive_offers(tx_event.clone()).await
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by OfferReviver: {:?}", error);
    }
}

/// EventAction for task status update
pub struct TaskStatusUpdater<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<SchedulerState<T, U>>,
    batch_size: usize,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskStatusUpdater<T, U> {
    pub fn new(state: Arc<SchedulerState<T, U>>) -> Self {
        Self::new_with_batch_size(state, 1024)
    }

    pub fn new_with_batch_size(
        state: Arc<SchedulerState<T, U>>,
        batch_size: usize,
    ) -> Self {
        Self { state, batch_size }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    event_loop::EventAction<TaskStatusUpdateEvent> for TaskStatusUpdater<T, U>
{
    fn on_start(&self) {
        info!("Starting TaskStatusUpdater");
    }

    fn on_stop(&self) {
        info!("Stopping TaskStatusUpdater");
    }

    async fn on_receive(
        &self,
        event: TaskStatusUpdateEvent,
        _tx_event: &Sender<TaskStatusUpdateEvent>,
        rx_event: &mut Receiver<TaskStatusUpdateEvent>,
    ) -> Result<()> {
        let stage_event_sender = event.stage_event_sender;
        let mut events = vec![(event.executor_id, event.tasks_status)];
        // Try to fetch events by non-blocking mode as many as possible
        loop {
            match rx_event.try_recv() {
                Ok(event) => {
                    events.push((event.executor_id, event.tasks_status));
                }
                Err(TryRecvError::Empty) => {
                    info!("{} task status update events fetched", events.len());
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Channel is closed and will exit the loop");
                    return Ok(());
                }
            }

            if events.len() >= self.batch_size {
                info!(
                    "{} task status update events as a full batch drained",
                    events.len()
                );
                break;
            }
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            match state.update_task_statuses(events).await {
                Ok(mut stage_events) => {
                    if state.config.is_push_staged_scheduling() {
                        stage_events.push(QueryStageSchedulerEvent::ReviveOffers);
                    }
                    for stage_event in stage_events {
                        if stage_event_sender
                            .post_event(stage_event.clone())
                            .await
                            .is_err()
                        {
                            error!(
                                "Fail to send back event {stage_event:?} to the channel"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Fail to batch update statuses: {}", e)
                }
            }
        });

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by TaskStatusUpdater: {:?}", error);
    }
}
