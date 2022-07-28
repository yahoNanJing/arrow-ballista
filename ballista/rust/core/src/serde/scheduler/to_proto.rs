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
use std::sync::atomic::Ordering;

use crate::error::{BallistaError, Result};
use crate::serde::protobuf::action::ActionType;
use crate::serde::protobuf::KeyValuePair;
use crate::serde::scheduler::{
    Action, ExecutorData, ExecutorMetadata, ExecutorResourcePair, ExecutorSpecification,
    ExecutorState, PartitionId, PartitionIds, PartitionLocation, PartitionStats,
    TaskDefinition,
};
use crate::serde::{protobuf, AsExecutionPlan, BallistaCodec};
use datafusion::physical_plan::Partitioning;
use datafusion_proto::logical_plan::AsLogicalPlan;

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action> {
        match self {
            Action::FetchPartition {
                job_id,
                stage_id,
                partition_id,
                path,
            } => Ok(protobuf::Action {
                action_type: Some(ActionType::FetchPartition(protobuf::FetchPartition {
                    job_id,
                    stage_id: stage_id as u32,
                    partition_id: partition_id as u32,
                    path,
                })),
                settings: vec![],
            }),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::PartitionId> for PartitionId {
    fn into(self) -> protobuf::PartitionId {
        protobuf::PartitionId {
            job_id: self.job_id,
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id as u32,
        }
    }
}

impl TryInto<protobuf::PartitionLocation> for PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PartitionLocation> {
        Ok(protobuf::PartitionLocation {
            partition_id: Some(self.partition_id.into()),
            executor_meta: Some(self.executor_meta.into()),
            partition_stats: Some(self.partition_stats.into()),
            path: self.path,
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::PartitionStats> for PartitionStats {
    fn into(self) -> protobuf::PartitionStats {
        let none_value = -1_i64;
        protobuf::PartitionStats {
            num_rows: self.num_rows.map(|n| n as i64).unwrap_or(none_value),
            num_batches: self.num_batches.map(|n| n as i64).unwrap_or(none_value),
            num_bytes: self.num_bytes.map(|n| n as i64).unwrap_or(none_value),
            column_stats: vec![],
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorMetadata> for ExecutorMetadata {
    fn into(self) -> protobuf::ExecutorMetadata {
        protobuf::ExecutorMetadata {
            id: self.id,
            host: self.host,
            port: self.port as u32,
            grpc_port: self.grpc_port as u32,
            specification: Some(self.specification.into()),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorSpecification> for ExecutorSpecification {
    fn into(self) -> protobuf::ExecutorSpecification {
        protobuf::ExecutorSpecification {
            resources: vec![protobuf::executor_resource::Resource::TaskSlots(
                self.task_slots,
            )]
            .into_iter()
            .map(|r| protobuf::ExecutorResource { resource: Some(r) })
            .collect(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorData> for ExecutorData {
    fn into(self) -> protobuf::ExecutorData {
        protobuf::ExecutorData {
            executor_id: self.executor_id,
            resources: vec![ExecutorResourcePair {
                total: protobuf::executor_resource::Resource::TaskSlots(
                    self.total_task_slots,
                ),
                available: protobuf::executor_resource::Resource::TaskSlots(
                    self.available_task_slots.load(Ordering::SeqCst),
                ),
            }]
            .into_iter()
            .map(|r| protobuf::ExecutorResourcePair {
                total: Some(protobuf::ExecutorResource {
                    resource: Some(r.total),
                }),
                available: Some(protobuf::ExecutorResource {
                    resource: Some(r.available),
                }),
            })
            .collect(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorState> for ExecutorState {
    fn into(self) -> protobuf::ExecutorState {
        protobuf::ExecutorState {
            metrics: vec![protobuf::executor_metric::Metric::AvailableMemory(
                self.available_memory_size,
            )]
            .into_iter()
            .map(|m| protobuf::ExecutorMetric { metric: Some(m) })
            .collect(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    TryInto<protobuf::TaskDefinition> for (TaskDefinition, BallistaCodec<T, U>)
{
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::TaskDefinition> {
        let task = self.0;
        let codec = self.1;

        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(task.plan.clone(), codec.physical_extension_codec())
            .and_then(|m| m.try_encode(&mut plan))?;
        let props = task
            .props
            .iter()
            .map(|(k, v)| KeyValuePair {
                key: k.to_owned(),
                value: v.to_owned(),
            })
            .collect::<Vec<_>>();

        Ok(protobuf::TaskDefinition {
            task_id: Some(task.task_id.into()),
            plan,
            session_id: task.session_id,
            props,
        })
    }
}

pub fn hash_partitioning_to_proto(
    output_partitioning: Option<&Partitioning>,
) -> Result<Option<protobuf::PhysicalHashRepartition>> {
    match output_partitioning {
        Some(Partitioning::Hash(exprs, partition_count)) => {
            Ok(Some(protobuf::PhysicalHashRepartition {
                hash_expr: exprs
                    .iter()
                    .map(|expr| expr.clone().try_into())
                    .collect::<Result<Vec<_>>>()?,
                partition_count: *partition_count as u64,
            }))
        }
        None => Ok(None),
        other => {
            return Err(BallistaError::General(format!(
                "scheduler::to_proto() invalid partitioning for ExecutePartition: {:?}",
                other
            )))
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::PartitionIds> for PartitionIds {
    fn into(self) -> protobuf::PartitionIds {
        protobuf::PartitionIds {
            job_id: self.job_id,
            stage_id: self.stage_id as u32,
            partition_ids: self
                .partition_ids
                .into_iter()
                .map(|partition_id| partition_id as u32)
                .collect(),
        }
    }
}
