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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ballista_core::serde::protobuf::ExecutorHeartbeat;
use ballista_core::serde::scheduler::{ExecutorData, ExecutorDataChange};
use log::{error, info, warn};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ExecutorManager {
    executors_heartbeat: Arc<RwLock<HashMap<String, ExecutorHeartbeat>>>,
    executors_data: Arc<RwLock<HashMap<String, ExecutorData>>>,
}

impl ExecutorManager {
    pub(crate) fn new() -> Self {
        Self {
            executors_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            executors_data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) {
        let mut executors_heartbeat = self.executors_heartbeat.write();
        executors_heartbeat.insert(heartbeat.executor_id.clone(), heartbeat);
    }

    pub(crate) fn get_executors_heartbeat(&self) -> Vec<ExecutorHeartbeat> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .map(|(_exec, heartbeat)| heartbeat.clone())
            .collect()
    }

    /// last_seen_ts_threshold is in seconds
    pub(crate) fn get_alive_executors(
        &self,
        last_seen_ts_threshold: u64,
    ) -> HashSet<String> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .filter_map(|(exec, heartbeat)| {
                (heartbeat.timestamp > last_seen_ts_threshold).then(|| exec.clone())
            })
            .collect()
    }

    #[allow(dead_code)]
    fn get_alive_executors_within_one_minute(&self) -> HashSet<String> {
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let last_seen_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(|| Duration::from_secs(0));
        self.get_alive_executors(last_seen_threshold.as_secs())
    }

    pub(crate) fn save_executor_data(&self, executor_data: ExecutorData) {
        let mut executors_data = self.executors_data.write();
        executors_data.insert(executor_data.executor_id.clone(), executor_data);
    }

    pub(crate) fn update_executor_data(&self, executor_data_change: &ExecutorDataChange) {
        let executors_data = self.executors_data.read();
        if let Some(executor_data) = executors_data.get(&executor_data_change.executor_id)
        {
            let mut cur_available_task_slots =
                executor_data.available_task_slots.load(Ordering::SeqCst);
            loop {
                let new_available_task_slots =
                    cur_available_task_slots as i32 + executor_data_change.task_slots;
                if new_available_task_slots < 0 {
                    error!(
                        "Available task slots {} for executor {} is less than 0",
                        new_available_task_slots, executor_data.executor_id
                    );
                    break;
                } else if let Err(cur_slots) =
                    executor_data.available_task_slots.compare_exchange(
                        cur_available_task_slots,
                        new_available_task_slots as u32,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                {
                    info!("Available task slots number has been changed from {} to {} before updating", cur_available_task_slots, cur_slots);
                    cur_available_task_slots = cur_slots;
                } else {
                    info!(
                        "available_task_slots for executor {} becomes {}",
                        executor_data.executor_id, new_available_task_slots
                    );
                    break;
                }
            }
        } else {
            warn!(
                "Could not find executor data for {}",
                executor_data_change.executor_id
            );
        }
    }

    pub(crate) fn get_executor_data(&self, executor_id: &str) -> Option<ExecutorData> {
        let executors_data = self.executors_data.read();
        executors_data.get(executor_id).cloned()
    }

    /// There are two checks:
    /// 1. firstly alive
    /// 2. secondly available task slots > 0
    #[cfg(not(test))]
    #[allow(dead_code)]
    pub(crate) fn get_available_executors_data(&self) -> Vec<(String, u32)> {
        let mut res: Vec<(String, u32)> = {
            let alive_executors = self.get_alive_executors_within_one_minute();
            let executors_data = self.executors_data.read();
            executors_data
                .iter()
                .filter_map(|(exec, data)| {
                    let available_task_slots =
                        data.available_task_slots.load(Ordering::SeqCst);
                    (available_task_slots > 0 && alive_executors.contains(exec))
                        .then(|| (data.executor_id.to_owned(), available_task_slots))
                })
                .collect()
        };
        res.sort_by(|a, b| Ord::cmp(&b.1, &a.1));
        res
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn get_available_executors_data(&self) -> Vec<(String, u32)> {
        let mut res: Vec<(String, u32)> = {
            let executors_data = self.executors_data.read();
            executors_data
                .iter()
                .filter_map(|(_exec, data)| {
                    let available_task_slots =
                        data.available_task_slots.load(Ordering::SeqCst);
                    (available_task_slots > 0)
                        .then(|| (data.executor_id.to_owned(), available_task_slots))
                })
                .collect()
        };
        res.sort_by(|a, b| Ord::cmp(&b.1, &a.1));
        res
    }
}
