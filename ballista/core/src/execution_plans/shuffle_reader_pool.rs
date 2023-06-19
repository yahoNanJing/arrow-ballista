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

use crate::client::BallistaClient;
use crate::error::BallistaError;
use crate::serde::scheduler::ExecutorMetadata;
use log::warn;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

lazy_static::lazy_static! {
    pub static ref SHUFFLE_READER_POOL: ShufflerReaderPool = ShufflerReaderPool::new();
}

#[derive(Default)]
pub struct ShufflerReaderPool {
    /// Cached connections
    /// key: executor id
    /// value: client connecting to the executor
    connections: Arc<RwLock<HashMap<String, BallistaClient>>>,
}

impl ShufflerReaderPool {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a [`BallistaClient`] for an executor from the cache pool.
    /// If not exist, create a new one and cache it.
    pub async fn get(
        &self,
        metadata: &ExecutorMetadata,
    ) -> Result<BallistaClient, BallistaError> {
        // Try to get from existing pool
        {
            let connections = self.connections.read().await;
            if let Some(client) = connections.get(&metadata.id) {
                return Ok(client.clone());
            }
        }

        // Try to create a new one and cache it
        {
            let mut connections = self.connections.write().await;

            // In case of that its already been inserted
            if let Some(client) = connections.get(&metadata.id) {
                return Ok(client.clone());
            }

            let client = BallistaClient::try_new(&metadata.host, metadata.port).await?;
            connections.insert(metadata.id.clone(), client.clone());
            Ok(client)
        }
    }

    /// Remove a [`BallistaClient`] for an executor.
    /// It may be invoked when finding the connection is invalid
    pub async fn remove(&self, metadata: &ExecutorMetadata) {
        let mut connections = self.connections.write().await;

        if connections.remove(&metadata.id).is_none() {
            warn!("Fail to find client for {} in the pool", &metadata.id);
        }
    }

    /// Get the cached total client number
    #[allow(dead_code)]
    pub async fn pool_length(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::error::BallistaError;
    use crate::execution_plans::shuffle_reader_pool::SHUFFLE_READER_POOL;
    use crate::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};

    #[tokio::test]
    async fn test_pool_work() -> Result<(), BallistaError> {
        let executor_meta = ExecutorMetadata {
            id: "1".to_string(),
            host: "127.0.0.1".to_string(),
            port: 50051,
            grpc_port: 50052,
            specification: ExecutorSpecification { task_slots: 1 },
        };

        assert!(SHUFFLE_READER_POOL.get(&executor_meta).await.is_err());

        assert_eq!(0, SHUFFLE_READER_POOL.pool_length().await);

        SHUFFLE_READER_POOL.remove(&executor_meta).await;

        // Assert that invalid cache is work.
        assert_eq!(0, SHUFFLE_READER_POOL.pool_length().await);

        Ok(())
    }
}
