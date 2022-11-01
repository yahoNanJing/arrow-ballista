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
//

//! Ballista scheduler specific configuration

use ballista_core::config::{ConfigEntry, ValidConfiguration, ValidConfigurationBuilder};
use ballista_core::error::Result;
use clap::ArgEnum;
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use std::fmt;

pub const BALLISTA_FINISHED_JOB_DATA_CLEANUP_DELAY_SECS: &str =
    "ballista.finished.job.data.cleanup.delay.seconds";
pub const BALLISTA_FINISHED_JOB_STATE_CLEANUP_DELAY_SECS: &str =
    "ballista.finished.job.state.cleanup.delay.seconds";
pub const BALLISTA_SCHEDULER_EVENT_LOOP_BUFFER_SIZE: &str =
    "ballista.scheduler.event.loop.buffer.size";
pub const BALLISTA_ADVERTISE_FLIGHT_RESULT_ROUTE_ENDPOINT: &str =
    "ballista.advertise.flight.result.route.endpoint";

/// Ballista configuration, mainly for the scheduling jobs and tasks
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerConfig {
    /// Settings stored in map for easy serde
    valid_config: ValidConfiguration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        // We should set all of the default values for the scheduler configs
        Self::with_settings(HashMap::new()).unwrap()
    }
}

impl SchedulerConfig {
    /// Create a configuration builder
    pub fn builder() -> SchedulerConfigBuilder {
        SchedulerConfigBuilder::default()
    }

    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        SchedulerConfigBuilder::with_settings(settings)
            .and_then(|builder| builder.build())
    }

    pub fn clean_up_interval_for_finished_job_data(&self) -> u64 {
        self.valid_config
            .get_usize_setting(BALLISTA_FINISHED_JOB_DATA_CLEANUP_DELAY_SECS)
            as u64
    }

    pub fn clean_up_interval_for_finished_job_state(&self) -> u64 {
        self.valid_config
            .get_usize_setting(BALLISTA_FINISHED_JOB_STATE_CLEANUP_DELAY_SECS)
            as u64
    }

    pub fn scheduler_event_loop_buffer_size(&self) -> usize {
        self.valid_config
            .get_usize_setting(BALLISTA_SCHEDULER_EVENT_LOOP_BUFFER_SIZE)
    }

    pub fn advertise_flight_result_route_endpoint(&self) -> Option<String> {
        let advertise_result_endpoint = self
            .valid_config
            .get_string_setting(BALLISTA_ADVERTISE_FLIGHT_RESULT_ROUTE_ENDPOINT);
        if advertise_result_endpoint.is_empty() {
            None
        } else {
            Some(advertise_result_endpoint)
        }
    }
}

/// Ballista configuration builder
#[derive(Default)]
pub struct SchedulerConfigBuilder {
    valid_config_builder: ValidConfigurationBuilder,
}

impl SchedulerConfigBuilder {
    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            valid_config_builder: ValidConfigurationBuilder::with_settings(settings),
        })
    }

    /// Create a new config with an additional setting
    pub fn set(&self, k: &str, v: &str) -> Self {
        Self {
            valid_config_builder: self.valid_config_builder.set(k, v),
        }
    }

    pub fn build(&self) -> Result<SchedulerConfig> {
        self.valid_config_builder
            .build(Self::valid_entries())
            .map(|valid_config| SchedulerConfig { valid_config })
    }

    /// All available configuration options
    pub fn valid_entries() -> Vec<ConfigEntry> {
        vec![
            ConfigEntry::new(BALLISTA_FINISHED_JOB_DATA_CLEANUP_DELAY_SECS.to_string(),
                             "Set the delayed seconds to cleanup finished job data. 0 means never do the cleanup".to_string(),
                             DataType::UInt64, Some("300".to_string())),
            ConfigEntry::new(BALLISTA_FINISHED_JOB_STATE_CLEANUP_DELAY_SECS.to_string(),
                             "Set the delayed seconds to cleanup finished job state stored in backend state store. 0 means never do the cleanup".to_string(),
                             DataType::UInt64, Some("3600".to_string())),
            ConfigEntry::new(BALLISTA_SCHEDULER_EVENT_LOOP_BUFFER_SIZE.to_string(),
                             "Set the buffer size for the scheduler event loop".to_string(),
                             DataType::UInt32, Some("10000".to_string())),
            ConfigEntry::new(BALLISTA_ADVERTISE_FLIGHT_RESULT_ROUTE_ENDPOINT.to_string(),
                             "Set the advertise route endpoint for the flight result".to_string(),
                             DataType::Utf8, Some("".to_string())),
        ]
    }
}

// an enum used to configure the executor slots policy
// needs to be visible to code generated by configure_me
#[derive(Clone, ArgEnum, Copy, Debug, serde::Deserialize)]
pub enum SlotsPolicy {
    Bias,
    RoundRobin,
    RoundRobinLocal,
}

impl SlotsPolicy {
    pub fn is_local(&self) -> bool {
        matches!(self, SlotsPolicy::RoundRobinLocal)
    }
}

impl std::str::FromStr for SlotsPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ArgEnum::from_str(s, true)
    }
}

impl parse_arg::ParseArgFromStr for SlotsPolicy {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The executor slots policy for the scheduler")
    }
}
