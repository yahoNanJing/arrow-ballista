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

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};
use arrow_flight::sql::ProstMessageExt;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;

use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::joins::utils::JoinSide;
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use prost::bytes::BufMut;
use prost::Message;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{convert::TryInto, io::Cursor};

pub use generated::ballista as protobuf;

pub mod generated;
pub mod physical_plan;
pub mod scheduler;

impl ProstMessageExt for protobuf::Action {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.Action"
    }

    fn as_any(&self) -> prost_types::Any {
        prost_types::Any {
            type_url: protobuf::Action::type_url().to_string(),
            value: self.encode_to_vec(),
        }
    }
}

pub fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);

    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::Internal(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

pub(crate) fn proto_error<S: Into<String>>(message: S) -> BallistaError {
    BallistaError::General(message.into())
}

pub trait AsExecutionPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self, BallistaError>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), BallistaError>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError>;

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self, BallistaError>
    where
        Self: Sized;
}

pub trait PhysicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError>;

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), BallistaError>;
}

#[derive(Debug, Clone)]
pub struct DefaultPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for DefaultPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        Err(BallistaError::NotImplemented(
            "PhysicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<(), BallistaError> {
        Err(BallistaError::NotImplemented(
            "PhysicalExtensionCodec is not provided".to_string(),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct BallistaCodec<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
    physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    logical_plan_repr: PhantomData<T>,
    physical_plan_repr: PhantomData<U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> Default
    for BallistaCodec<T, U>
{
    fn default() -> Self {
        Self {
            logical_extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            physical_extension_codec: Arc::new(DefaultPhysicalExtensionCodec {}),
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> BallistaCodec<T, U> {
    pub fn new(
        logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
        physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        Self {
            logical_extension_codec,
            physical_extension_codec,
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }

    pub fn logical_extension_codec(&self) -> &dyn LogicalExtensionCodec {
        self.logical_extension_codec.as_ref()
    }

    pub fn physical_extension_codec(&self) -> &dyn PhysicalExtensionCodec {
        self.physical_extension_codec.as_ref()
    }
}

#[macro_export]
macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            Ok(field
                .try_into()
                .map_err(|_| proto_error("Failed to convert!"))?)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! into_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            Ok(field.into())
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

impl From<protobuf::JoinSide> for JoinSide {
    fn from(t: protobuf::JoinSide) -> Self {
        match t {
            protobuf::JoinSide::LeftSide => JoinSide::Left,
            protobuf::JoinSide::RightSide => JoinSide::Right,
        }
    }
}

impl From<JoinSide> for protobuf::JoinSide {
    fn from(t: JoinSide) -> Self {
        match t {
            JoinSide::Left => protobuf::JoinSide::LeftSide,
            JoinSide::Right => protobuf::JoinSide::RightSide,
        }
    }
}

fn byte_to_string(b: u8) -> Result<String, BallistaError> {
    let b = &[b];
    let b = std::str::from_utf8(b)
        .map_err(|_| BallistaError::General("Invalid CSV delimiter".to_owned()))?;
    Ok(b.to_owned())
}

fn str_to_byte(s: &str) -> Result<u8, BallistaError> {
    if s.len() != 1 {
        return Err(BallistaError::General("Invalid CSV delimiter".to_owned()));
    }
    Ok(s.as_bytes()[0])
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::error::DataFusionError;
    use datafusion::execution::context::{QueryPlanner, SessionState, TaskContext};
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};

    use datafusion::physical_plan::expressions::PhysicalSortExpr;
    use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner};
    use datafusion::physical_plan::{
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalPlanner,
        SendableRecordBatchStream, Statistics,
    };
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use prost::Message;
    use std::any::Any;

    use datafusion::common::DFSchemaRef;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::{
        col, Expr, Extension, LogicalPlan, UserDefinedLogicalNode,
    };
    use datafusion_proto::from_proto::parse_expr;
    use std::convert::TryInto;
    use std::fmt;
    use std::fmt::{Debug, Formatter};
    use std::ops::Deref;
    use std::sync::Arc;

    pub mod proto {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopKPlanProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,

            #[prost(message, optional, tag = "2")]
            pub expr: ::core::option::Option<datafusion_proto::protobuf::LogicalExprNode>,
        }

        #[derive(Clone, Eq, PartialEq, ::prost::Message)]
        pub struct TopKExecProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,
        }
    }

    use crate::error::BallistaError;
    use crate::serde::protobuf::PhysicalPlanNode;
    use crate::serde::{
        AsExecutionPlan, AsLogicalPlan, LogicalExtensionCodec, PhysicalExtensionCodec,
    };
    use crate::utils::with_object_store_provider;
    use datafusion_proto::protobuf::LogicalPlanNode;
    use proto::{TopKExecProto, TopKPlanProto};

    struct TopKPlanNode {
        k: usize,
        input: LogicalPlan,
        /// The sort expression (this example only supports a single sort
        /// expr)
        expr: Expr,
    }

    impl TopKPlanNode {
        pub fn new(k: usize, input: LogicalPlan, expr: Expr) -> Self {
            Self { k, input, expr }
        }
    }

    impl Debug for TopKPlanNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            self.fmt_for_explain(f)
        }
    }

    impl UserDefinedLogicalNode for TopKPlanNode {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        /// Schema for TopK is the same as the input
        fn schema(&self) -> &DFSchemaRef {
            self.input.schema()
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![self.expr.clone()]
        }

        /// For example: `TopK: k=10`
        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TopK: k={}", self.k)
        }

        fn from_template(
            &self,
            exprs: &[Expr],
            inputs: &[LogicalPlan],
        ) -> Arc<dyn UserDefinedLogicalNode> {
            assert_eq!(inputs.len(), 1, "input size inconsistent");
            assert_eq!(exprs.len(), 1, "expression size inconsistent");
            Arc::new(TopKPlanNode {
                k: self.k,
                input: inputs[0].clone(),
                expr: exprs[0].clone(),
            })
        }
    }

    struct TopKExec {
        input: Arc<dyn ExecutionPlan>,
        /// The maxium number of values
        k: usize,
    }

    impl TopKExec {
        pub fn new(k: usize, input: Arc<dyn ExecutionPlan>) -> Self {
            Self { input, k }
        }
    }

    impl Debug for TopKExec {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "TopKExec")
        }
    }

    impl ExecutionPlan for TopKExec {
        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn required_input_distribution(&self) -> Vec<Distribution> {
            vec![Distribution::SinglePartition]
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.input.clone()]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(TopKExec {
                input: children[0].clone(),
                k: self.k,
            }))
        }

        /// Execute one partition and return an iterator over RecordBatch
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::error::Result<SendableRecordBatchStream> {
            Err(DataFusionError::NotImplemented(
                "not implemented".to_string(),
            ))
        }

        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default => {
                    write!(f, "TopKExec: k={}", self.k)
                }
            }
        }

        fn statistics(&self) -> Statistics {
            // to improve the optimizability of this plan
            // better statistics inference could be provided
            Statistics::default()
        }
    }

    struct TopKPlanner {}

    #[async_trait]
    impl ExtensionPlanner for TopKPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            node: &dyn UserDefinedLogicalNode,
            logical_inputs: &[&LogicalPlan],
            physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(
                if let Some(topk_node) = node.as_any().downcast_ref::<TopKPlanNode>() {
                    assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                    assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                    // figure out input name
                    Some(Arc::new(TopKExec {
                        input: physical_inputs[0].clone(),
                        k: topk_node.k,
                    }))
                } else {
                    None
                },
            )
        }
    }

    struct TopKQueryPlanner {}

    #[async_trait]
    impl QueryPlanner for TopKQueryPlanner {
        /// Given a `LogicalPlan` created from above, create an
        /// `ExecutionPlan` suitable for execution
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session_state: &SessionState,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            // Teach the default physical planner how to plan TopK nodes.
            let physical_planner =
                DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                    TopKPlanner {},
                )]);
            // Delegate most work of physical planning to the default physical planner
            physical_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        }
    }

    #[derive(Debug)]
    pub struct TopKExtensionCodec {}

    impl LogicalExtensionCodec for TopKExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[LogicalPlan],
            ctx: &SessionContext,
        ) -> Result<Extension, DataFusionError> {
            if let Some((input, _)) = inputs.split_first() {
                let proto = TopKPlanProto::decode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to decode logical plan: {:?}",
                        e
                    ))
                })?;

                if let Some(expr) = proto.expr.as_ref() {
                    let node = TopKPlanNode::new(
                        proto.k as usize,
                        input.clone(),
                        parse_expr(expr, ctx)?,
                    );

                    Ok(Extension {
                        node: Arc::new(node),
                    })
                } else {
                    Err(DataFusionError::Plan("invalid plan, no expr".to_string()))
                }
            } else {
                Err(DataFusionError::Plan("invalid plan, no input".to_string()))
            }
        }

        fn try_encode(
            &self,
            node: &Extension,
            buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            if let Some(exec) = node.node.as_any().downcast_ref::<TopKPlanNode>() {
                let proto = TopKPlanProto {
                    k: exec.k as u64,
                    expr: Some((&exec.expr).try_into()?),
                };

                proto.encode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to encode logical plan: {:?}",
                        e
                    ))
                })?;

                Ok(())
            } else {
                Err(DataFusionError::Plan("unsupported plan type".to_string()))
            }
        }

        fn try_decode_table_provider(
            &self,
            _buf: &[u8],
            _schema: SchemaRef,
            _ctx: &SessionContext,
        ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
            Err(DataFusionError::Internal(
                "unsupported plan type".to_string(),
            ))
        }

        fn try_encode_table_provider(
            &self,
            _node: Arc<dyn TableProvider>,
            _buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            Err(DataFusionError::Internal(
                "unsupported plan type".to_string(),
            ))
        }
    }

    impl PhysicalExtensionCodec for TopKExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _registry: &dyn FunctionRegistry,
        ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
            if let Some((input, _)) = inputs.split_first() {
                let proto = TopKExecProto::decode(buf).map_err(|e| {
                    BallistaError::Internal(format!(
                        "failed to decode execution plan: {:?}",
                        e
                    ))
                })?;
                Ok(Arc::new(TopKExec::new(proto.k as usize, input.clone())))
            } else {
                Err(BallistaError::from("invalid plan, no input".to_string()))
            }
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> Result<(), BallistaError> {
            if let Some(exec) = node.as_any().downcast_ref::<TopKExec>() {
                let proto = TopKExecProto { k: exec.k as u64 };

                proto.encode(buf).map_err(|e| {
                    BallistaError::Internal(format!(
                        "failed to encode execution plan: {:?}",
                        e
                    ))
                })?;

                Ok(())
            } else {
                Err(BallistaError::from("unsupported plan type".to_string()))
            }
        }
    }

    #[tokio::test]
    async fn test_extension_plan() -> crate::error::Result<()> {
        let runtime = Arc::new(
            RuntimeEnv::new(with_object_store_provider(RuntimeConfig::default()))
                .unwrap(),
        );
        let session_state =
            SessionState::with_config_rt(SessionConfig::new(), runtime.clone())
                .with_query_planner(Arc::new(TopKQueryPlanner {}));

        let ctx = SessionContext::with_state(session_state);

        let scan = ctx
            .read_csv("tests/customer.csv", CsvReadOptions::default())
            .await?
            .to_logical_plan()?;

        let topk_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(TopKPlanNode::new(3, scan, col("revenue"))),
        });

        let topk_exec = ctx.create_physical_plan(&topk_plan).await?;

        let extension_codec = TopKExtensionCodec {};

        let proto = LogicalPlanNode::try_from_logical_plan(&topk_plan, &extension_codec)?;
        let logical_round_trip = proto.try_into_logical_plan(&ctx, &extension_codec)?;

        assert_eq!(
            format!("{:?}", topk_plan),
            format!("{:?}", logical_round_trip)
        );

        let proto = PhysicalPlanNode::try_from_physical_plan(
            topk_exec.clone(),
            &extension_codec,
        )?;
        let physical_round_trip =
            proto.try_into_physical_plan(&ctx, runtime.deref(), &extension_codec)?;

        assert_eq!(
            format!("{:?}", topk_exec),
            format!("{:?}", physical_round_trip)
        );

        Ok(())
    }
}
