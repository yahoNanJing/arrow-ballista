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

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_proto::logical_plan::AsLogicalPlan;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::{BallistaError, Result};
use crate::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use crate::serde::protobuf::action::ActionType;
use crate::serde::protobuf::{KeyValuePair, PhysicalHashRepartition};
use crate::serde::scheduler::{
    Action, ExecutorData, ExecutorMetadata, ExecutorSpecification, ExecutorState,
    PartitionId, PartitionIds, PartitionLocation, PartitionStats, SharedTaskContext,
    SimpleFunctionRegistry,
};
use crate::serde::{protobuf, AsExecutionPlan, BallistaCodec};

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action> {
        match self.action_type {
            Some(ActionType::FetchPartition(fetch)) => Ok(Action::FetchPartition {
                job_id: fetch.job_id,
                stage_id: fetch.stage_id as usize,
                partition_id: fetch.partition_id as usize,
                path: fetch.path,
            }),
            _ => Err(BallistaError::General(
                "scheduler::from_proto(Action) invalid or missing action".to_owned(),
            )),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<PartitionId> for protobuf::PartitionId {
    fn into(self) -> PartitionId {
        PartitionId::new(
            &self.job_id,
            self.stage_id as usize,
            self.partition_id as usize,
        )
    }
}

#[allow(clippy::from_over_into)]
impl Into<PartitionStats> for protobuf::PartitionStats {
    fn into(self) -> PartitionStats {
        PartitionStats::new(
            foo(self.num_rows),
            foo(self.num_batches),
            foo(self.num_bytes),
        )
    }
}

fn foo(n: i64) -> Option<u64> {
    if n < 0 {
        None
    } else {
        Some(n as u64)
    }
}

impl TryInto<PartitionLocation> for protobuf::PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionLocation> {
        Ok(PartitionLocation {
            partition_id: self
                .partition_id
                .ok_or_else(|| {
                    BallistaError::General(
                        "partition_id in PartitionLocation is missing.".to_owned(),
                    )
                })?
                .into(),
            executor_meta: self
                .executor_meta
                .ok_or_else(|| {
                    BallistaError::General(
                        "executor_meta in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            partition_stats: self
                .partition_stats
                .ok_or_else(|| {
                    BallistaError::General(
                        "partition_stats in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            path: self.path,
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorMetadata> for protobuf::ExecutorMetadata {
    fn into(self) -> ExecutorMetadata {
        ExecutorMetadata {
            id: self.id,
            host: self.host,
            port: self.port as u16,
            grpc_port: self.grpc_port as u16,
            specification: self.specification.unwrap().into(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorSpecification> for protobuf::ExecutorSpecification {
    fn into(self) -> ExecutorSpecification {
        let mut ret = ExecutorSpecification { task_slots: 0 };
        for resource in self.resources {
            if let Some(protobuf::executor_resource::Resource::TaskSlots(task_slots)) =
                resource.resource
            {
                ret.task_slots = task_slots;
                break;
            }
        }
        ret
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorData> for protobuf::ExecutorData {
    fn into(self) -> ExecutorData {
        let mut ret = ExecutorData {
            executor_id: self.executor_id,
            total_task_slots: 0,
            available_task_slots: Arc::new(AtomicU32::new(0)),
        };
        for resource in self.resources {
            if let Some(task_slots) = resource.total {
                if let Some(protobuf::executor_resource::Resource::TaskSlots(
                    task_slots,
                )) = task_slots.resource
                {
                    ret.total_task_slots = task_slots
                }
            };
            if let Some(task_slots) = resource.available {
                if let Some(protobuf::executor_resource::Resource::TaskSlots(
                    task_slots,
                )) = task_slots.resource
                {
                    ret.available_task_slots.store(task_slots, Ordering::SeqCst);
                }
            };
        }
        ret
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorState> for protobuf::ExecutorState {
    fn into(self) -> ExecutorState {
        let mut ret = ExecutorState {
            available_memory_size: u64::MAX,
        };
        for metric in self.metrics {
            if let Some(protobuf::executor_metric::Metric::AvailableMemory(
                available_memory_size,
            )) = metric.metric
            {
                ret.available_memory_size = available_memory_size
            }
        }
        ret
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TryInto<SharedTaskContext>
    for (
        protobuf::TaskDefinition,
        SimpleFunctionRegistry,
        &RuntimeEnv,
        &BallistaCodec<T, U>,
    )
{
    type Error = BallistaError;

    fn try_into(self) -> Result<SharedTaskContext> {
        let task = self.0;
        let function_registry = self.1;
        let runtime = self.2;
        let codec = self.3;

        create_shared_task_context(
            &task.plan,
            task.output_partitioning,
            task.session_id,
            task.props,
            function_registry,
            runtime,
            codec,
        )
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TryInto<SharedTaskContext>
    for (
        protobuf::MultiTaskDefinition,
        SimpleFunctionRegistry,
        &RuntimeEnv,
        &BallistaCodec<T, U>,
    )
{
    type Error = BallistaError;

    fn try_into(self) -> Result<SharedTaskContext> {
        let task = self.0;
        let function_registry = self.1;
        let runtime = self.2;
        let codec = self.3;

        create_shared_task_context(
            &task.plan,
            task.output_partitioning,
            task.session_id,
            task.props,
            function_registry,
            runtime,
            codec,
        )
    }
}

fn create_shared_task_context<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    encoded_plan: &[u8],
    partitioning: Option<PhysicalHashRepartition>,
    session_id: String,
    task_props: Vec<KeyValuePair>,
    function_registry: SimpleFunctionRegistry,
    runtime: &RuntimeEnv,
    codec: &BallistaCodec<T, U>,
) -> Result<SharedTaskContext> {
    let plan = U::try_decode(encoded_plan).and_then(|proto| {
        proto.try_into_physical_plan(
            &function_registry,
            runtime,
            codec.physical_extension_codec(),
        )
    })?;

    let output_partitioning = parse_protobuf_hash_partitioning(
        partitioning.as_ref(),
        &function_registry,
        plan.schema().as_ref(),
    )?;
    let mut props = HashMap::new();
    for kv_pair in task_props {
        props.insert(kv_pair.key, kv_pair.value);
    }

    Ok(SharedTaskContext {
        plan,
        output_partitioning,
        session_id,
        props,
        function_registry,
    })
}

#[allow(clippy::from_over_into)]
impl Into<PartitionIds> for protobuf::PartitionIds {
    fn into(self) -> PartitionIds {
        PartitionIds {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id as usize,
            partition_ids: self
                .partition_ids
                .into_iter()
                .map(|partition_id| partition_id as usize)
                .collect(),
        }
    }
}
