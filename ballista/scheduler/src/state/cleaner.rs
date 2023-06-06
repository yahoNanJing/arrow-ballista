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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop;

use crate::scheduler_server::event::{JobDataCleanupEvent, JobStateCleanupEvent};
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
                    info!("Fetched {} events", events.len());
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Channel is closed and will exit the loop");
                    return Ok(());
                }
            }
            if events.len() >= self.batch_size {
                info!("Fetched a full batch {} events to deal with", events.len());
                break;
            }
        }

        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut jobs_to_clean = HashSet::new();
        let mut min_deadline = now_epoch_ts + self.sleep_ts;
        for event in events {
            match event {
                JobDataCleanupEvent::Immediate { job_id } => {
                    jobs_to_clean.insert(job_id);
                }
                JobDataCleanupEvent::Delayed { job_id, deadline } => {
                    if deadline <= now_epoch_ts {
                        jobs_to_clean.insert(job_id);
                    } else {
                        if deadline < min_deadline {
                            min_deadline = deadline;
                        }
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
                }
            }
        }

        if jobs_to_clean.is_empty() {
            assert!(min_deadline > now_epoch_ts);
            tokio::time::sleep(Duration::from_secs(min_deadline - now_epoch_ts)).await;
        } else {
            let state = self.state.clone();
            tokio::spawn(async move {
                state
                    .executor_manager
                    .clean_up_job_data(Vec::from_iter(jobs_to_clean.into_iter()))
                    .await;
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
                    info!("Fetched {} events", events.len());
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Channel is closed and will exit the loop");
                    return Ok(());
                }
            }
            if events.len() >= self.batch_size {
                info!("Fetched a full batch {} events to deal with", events.len());
                break;
            }
        }

        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut jobs_to_clean = HashSet::new();
        let mut min_deadline = now_epoch_ts + self.sleep_ts;
        for event in events {
            match event {
                JobStateCleanupEvent::Immediate { job_id } => {
                    jobs_to_clean.insert(job_id);
                }
                JobStateCleanupEvent::Delayed { job_id, deadline } => {
                    if deadline <= now_epoch_ts {
                        jobs_to_clean.insert(job_id);
                    } else {
                        if deadline < min_deadline {
                            min_deadline = deadline;
                        }
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
                }
            }
        }

        if jobs_to_clean.is_empty() {
            assert!(min_deadline > now_epoch_ts);
            tokio::time::sleep(Duration::from_secs(min_deadline - now_epoch_ts)).await;
        } else {
            self.state
                .task_manager
                .remove_jobs(Vec::from_iter(jobs_to_clean.into_iter()))
                .await;
        }

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by JobStateCleaner: {:?}", error);
    }
}
