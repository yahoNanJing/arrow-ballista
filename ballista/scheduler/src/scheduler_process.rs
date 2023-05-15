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

use anyhow::{Context, Result};
#[cfg(feature = "flight-sql")]
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::future::{self, Either, TryFutureExt};
use hyper::{server::conn::AddrStream, service::make_service_fn, Server};
use log::{info, warn};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport::server::Connected;
use tower::Service;

use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::create_grpc_server;
use ballista_core::BALLISTA_VERSION;

use crate::api::{get_routes, EitherBody, Error};
use crate::cluster::BallistaCluster;
use crate::config::{ClusterStorageConfig, SchedulerConfig};
use crate::flight_sql::FlightSqlServiceImpl;
use crate::metrics::default_metrics_collector;
use crate::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use crate::scheduler_server::SchedulerServer;
use crate::zk::SchedulerZkLeaderService;

pub async fn start_server(
    cluster: BallistaCluster,
    addr: SocketAddr,
    config: SchedulerConfig,
) -> Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    // Should only call SchedulerServer::new() once in the process
    info!(
        "Starting Scheduler grpc server with task scheduling policy of {:?}",
        config.scheduling_policy
    );

    let metrics_collector = default_metrics_collector()?;

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            config.scheduler_name(),
            cluster,
            BallistaCodec::default(),
            config.clone(),
            metrics_collector,
        );

    scheduler_server.init().await?;

    if config.zk_address.is_some()
        && config.zk_leader_election_path.is_some()
        && config.zk_leader_host_path.is_some()
        && config.zk_session_timeout > 0
        && config.cluster_storage == ClusterStorageConfig::Memory
    {
        let scheduler_address =
            config.external_host.clone() + ":" + &config.bind_port.clone().to_string();
        info!(
            "Use the zk to do leader election: {:?}, {:?}, {:?}, {:?}, {:?}",
            config.zk_address,
            scheduler_address,
            config.zk_leader_election_path,
            config.zk_leader_host_path,
            config.zk_session_timeout
        );
        let zk_session_timeout = Duration::from_secs(config.zk_session_timeout.clone());
        let zk_address = config.zk_address.clone().unwrap();
        let leader_election_path = config.zk_leader_election_path.clone().unwrap();
        let leader_host_path = config.zk_leader_host_path.clone().unwrap();
        let listener = Box::new(scheduler_server.clone());
        let mut zk_leader_election = SchedulerZkLeaderService::new(
            leader_host_path,
            leader_election_path,
            scheduler_address,
            zk_session_timeout,
            zk_address,
            listener,
        );
        tokio::spawn(async move {
            info!("Starting zk leader election");
            zk_leader_election.start_election().await;
        });
    } else {
        warn!(
            "Can't use the zk to do leader election: {:?}, {:?}, {:?}, {:?}, {:?}",
            config.zk_address,
            config.zk_leader_election_path,
            config.zk_leader_host_path,
            config.zk_session_timeout,
            config.cluster_storage,
        );
    }

    Server::bind(&addr)
        .serve(make_service_fn(move |request: &AddrStream| {
            let scheduler_grpc_server =
                SchedulerGrpcServer::new(scheduler_server.clone());

            let keda_scaler = ExternalScalerServer::new(scheduler_server.clone());

            let tonic_builder = create_grpc_server()
                .add_service(scheduler_grpc_server)
                .add_service(keda_scaler);

            #[cfg(feature = "flight-sql")]
            let tonic_builder = tonic_builder.add_service(FlightServiceServer::new(
                FlightSqlServiceImpl::new(scheduler_server.clone()),
            ));

            let mut tonic = tonic_builder.into_service();

            let mut warp = warp::service(get_routes(scheduler_server.clone()));

            let connect_info = request.connect_info();
            future::ok::<_, Infallible>(tower::service_fn(
                move |req: hyper::Request<hyper::Body>| {
                    // Set the connect info from hyper to tonic
                    let (mut parts, body) = req.into_parts();
                    parts.extensions.insert(connect_info.clone());
                    let req = http::Request::from_parts(parts, body);

                    if req.uri().path().starts_with("/api") {
                        return Either::Left(
                            warp.call(req)
                                .map_ok(|res| res.map(EitherBody::Left))
                                .map_err(Error::from),
                        );
                    }

                    Either::Right(
                        tonic
                            .call(req)
                            .map_ok(|res| res.map(EitherBody::Right))
                            .map_err(Error::from),
                    )
                },
            ))
        }))
        .await
        .context("Could not start grpc server")
}
