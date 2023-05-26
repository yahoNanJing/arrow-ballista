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

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{broadcast, mpsc};

/// Listens for the server shutdown signal(copied from mini-redis example).
///
/// Shutdown is signalled using a `broadcast::Receiver`. Only a single value is
/// ever sent. Once a value has been sent via the broadcast channel, the server
/// should shutdown.
///
/// The `Shutdown` struct listens for the signal and tracks that the signal has
/// been received. Callers may query for whether the shutdown signal has been
/// received or not.
#[derive(Debug)]
pub struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: AtomicBool,

    /// The receive half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: AtomicBool::new(false),
            notify,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.is_shutdown() {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        // Remember that the signal has been received.
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Sync method to get the shutdown result
    pub fn try_recv(&mut self) -> bool {
        if self.is_shutdown() {
            return true;
        }

        match self.notify.try_recv() {
            Ok(_) => {
                // get shutdown message
            }
            Err(TryRecvError::Empty) => {
                // no value received, should not be shut down
                return false;
            }
            Err(_) => {
                // closed or lagged, which indicate shutdown value received
            }
        }

        self.shutdown.store(true, Ordering::SeqCst);

        true
    }
}

pub struct ShutdownNotifier {
    /// Broadcasts a shutdown signal to all related components.
    pub notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for
    /// related components to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all shutdown processing completing.
    pub shutdown_complete_rx: mpsc::Receiver<()>,

    pub shutdown_complete_tx: mpsc::Sender<()>,
}

impl ShutdownNotifier {
    /// Create a new ShutdownNotifier instance
    pub fn new() -> Self {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        Self {
            notify_shutdown,
            shutdown_complete_rx,
            shutdown_complete_tx,
        }
    }

    /// Subscribe for shutdown notification
    pub fn subscribe_for_shutdown(&self) -> Shutdown {
        Shutdown::new(self.notify_shutdown.subscribe())
    }
}

impl Default for ShutdownNotifier {
    fn default() -> Self {
        ShutdownNotifier::new()
    }
}
