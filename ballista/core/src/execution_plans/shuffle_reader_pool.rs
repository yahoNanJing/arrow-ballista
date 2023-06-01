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
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static::lazy_static! {
    pub static ref SHUFFLE_READER_POOL: ShufflerReaderPool = ShufflerReaderPool::new();
}

#[derive(Default)]
pub struct ShufflerReaderPool {
    /// Cached connections
    /// key: address (e.g. "http://executor-1:50050")
    /// value: CachedConnection
    ///
    /// Note: Use sync (parking_log) mutex because it is always held
    /// for a very short period of time, and any actual connection (and
    /// waiting) is done in CachedConnection
    connections: parking_lot::Mutex<HashMap<String, CachedConnection>>,
}

impl ShufflerReaderPool {
    /// Create new client.
    pub fn new() -> Self {
        Self::default()
    }

    /// Establish connection to given addr.
    pub async fn connect(
        &self,
        address: Arc<str>,
    ) -> Result<BallistaClient, BallistaError> {
        let cached_connection = {
            let mut connections = self.connections.lock();
            if let Some(cached_connection) = connections.get(address.as_ref()) {
                cached_connection.clone()
            } else {
                // cache entry not exit: need to make a new one;
                let cached_connection = CachedConnection::new(&address);
                connections.insert(address.to_string(), cached_connection.clone());
                cached_connection
            }
        };
        cached_connection.connect().await
    }

    /// Destruct the connection to given addr.
    pub async fn invalidate_connection(&self, address: Arc<str>) {
        let maybe_conn = self.connections.lock().remove(address.as_ref());

        if let Some(conn) = maybe_conn {
            conn.close().await;
        }
    }
}

#[derive(Clone)]
struct CachedConnection {
    executor_address: Arc<str>,
    /// Real async mutex to
    maybe_connection: Arc<tokio::sync::Mutex<Option<BallistaClient>>>,
}

impl CachedConnection {
    fn new(address: &Arc<str>) -> Self {
        Self {
            executor_address: Arc::clone(address),
            maybe_connection: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Return the underlying connection, creating it if needed, Notice this is the major block part.
    async fn connect(&self) -> Result<BallistaClient, BallistaError> {
        let mut maybe_connection = self.maybe_connection.lock().await;

        let address = self.executor_address.as_ref();

        if let Some(client) = maybe_connection.as_ref() {
            info!("[Shuffle reader] Reusing connection to {}", address);

            Ok(client.clone())
        } else {
            info!("[Shuffle reader] Creating new connection to {}", address);

            let client = BallistaClient::try_new_with_addr(address.into()).await
                .map_err(|error| match error {
                    // map grpc connection error to partition fetch error.
                    BallistaError::GrpcConnectionError(msg) => BallistaError::GrpcConnectionError(
                        format!("[Shuffle reader] creating new connection to {} fail in pool. error msg: {}", address, msg)
                    ),
                    other => other,
                })?;

            // Set the cache entry
            *maybe_connection = Some(client.clone());
            Ok(client)
        }
    }

    /// Close connection
    async fn close(&self) {
        let mut maybe_connection = self.maybe_connection.lock().await;

        // dropping the channel to close it.
        maybe_connection.take();
    }
}

#[cfg(test)]
mod tests {
    use crate::error::BallistaError;
    use crate::execution_plans::shuffle_reader_pool::SHUFFLE_READER_POOL;
    use std::sync::Arc;

    const ADDR: &str = "http:://127.0.0.1:50050";

    #[tokio::test]
    async fn test_pool_work() -> Result<(), BallistaError> {
        // return error but not mind just test cache
        let _res1 = SHUFFLE_READER_POOL.connect(Arc::from(ADDR)).await;

        let _res2 = SHUFFLE_READER_POOL.connect(Arc::from(ADDR)).await;

        let length = SHUFFLE_READER_POOL.connections.lock().len();

        // Assert that cache is work.
        assert_eq!(length, 1);

        SHUFFLE_READER_POOL
            .invalidate_connection(Arc::from(ADDR))
            .await;

        let length = SHUFFLE_READER_POOL.connections.lock().len();
        // Assert that invalid cache is work.
        assert_eq!(length, 0);

        Ok(())
    }
}
