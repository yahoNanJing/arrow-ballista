use crate::executor_server::ExecutorServer;
use crate::shutdown::Shutdown;
use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::utils::create_grpc_client_connection;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{error, info, warn};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use zookeeper::{WatchedEvent, WatchedEventType, Watcher, ZkState, ZooKeeper};

#[tonic::async_trait]
pub trait ExecutorStateChangeListener: Send + Sync {
    async fn change_scheduler(
        &mut self,
        scheduler_url: String,
    ) -> Result<(), BallistaError>;
}

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorStateChangeListener
    for ExecutorServer<T, U>
{
    async fn change_scheduler(
        &mut self,
        scheduler_url: String,
    ) -> Result<(), BallistaError> {
        let mut write = self.scheduler_from_zk.write().await;

        let new_scheduler_url = if write.is_none() {
            // there is no previous scheduler
            let scheduler_url = format!("http://{}", scheduler_url.clone());
            Some(scheduler_url)
        } else {
            // need to create a new scheduler client
            let scheduler_url = format!("http://{}", scheduler_url.clone());
            Some(scheduler_url)
        };

        match new_scheduler_url {
            None => {
                error!("Can't find the new scheduler url");
                Err(BallistaError::Internal(
                    "Can't find the new scheduler url".to_string(),
                ))
            }
            Some(connection_scheduler_url) => {
                info!(
                    "Create the new scheduler connection: {:?}",
                    connection_scheduler_url
                );
                let new_connection = match create_grpc_client_connection(
                    connection_scheduler_url.clone(),
                )
                .await
                {
                    Ok(connection) => connection,
                    Err(grpc_error) => {
                        warn!("Fail to create grpc scheduler connection with {:?}, the url is {:?}",
                            grpc_error, connection_scheduler_url);
                        return Err(BallistaError::TonicError(grpc_error));
                    }
                };
                // clear executor tasks
                self.clear_executor_tasks();
                // create the new connection
                let mut new_scheduler_client = SchedulerGrpcClient::new(new_connection);
                // register to the new scheduler
                match self
                    .register_to_new_scheduler(
                        &mut new_scheduler_client,
                        &connection_scheduler_url,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(register_error) => {
                        warn!(
                            "Fail to register to the new scheduler {:?}",
                            connection_scheduler_url
                        );
                        return Err(register_error);
                    }
                };
                // overwrite the lock value
                *write = Some((scheduler_url.clone(), new_scheduler_client));
                Ok(())
            }
        }
    }
}

pub struct SchedulerChangeWatcher {
    scheduler_changed: Arc<AtomicBool>,
}

impl SchedulerChangeWatcher {
    pub fn new(scheduler_changed: Arc<AtomicBool>) -> Self {
        SchedulerChangeWatcher { scheduler_changed }
    }
}

impl Watcher for SchedulerChangeWatcher {
    fn handle(&self, event: WatchedEvent) {
        info!("Watch event for the executor: {:?}", event);
        match event.event_type {
            WatchedEventType::NodeDataChanged => {
                info!("Get data changed event for the executor");
                self.scheduler_changed.store(true, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

pub struct DefaultExecutorZkWatcher;

impl Watcher for DefaultExecutorZkWatcher {
    fn handle(&self, event: WatchedEvent) {
        info!("Default DefaultExecutorZkWatcher: {:?}", event);
    }
}

pub struct ExecutorZkService {
    leader_host_path: String,
    leader_scheduler_url: Option<String>,
    leader_scheduler_changed: Arc<AtomicBool>,
    zk_is_connected: Arc<AtomicBool>,
    zk_client: Option<Arc<ZooKeeper>>,
    // the timeout for zookeeper client
    zk_session_timeout: Duration,
    zk_address: String,
    listener: Option<Box<dyn ExecutorStateChangeListener>>,
    wait_time: Duration,
}

impl Debug for ExecutorZkService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorZkService")
            .field("zk_is_connected", &self.zk_is_connected)
            .field("leader_scheduler_changed", &self.leader_scheduler_changed)
            .field("zk_session_timeout", &self.zk_session_timeout)
            .field("wait_time", &self.wait_time)
            .field("leader_scheduler_url", &self.leader_scheduler_url)
            .finish()
    }
}

impl ExecutorZkService {
    pub fn new(
        leader_host_path: String,
        zk_session_timeout: Duration,
        zk_address: String,
    ) -> Self {
        let rand = rand::random::<f32>();
        let wait_time = (zk_session_timeout.as_secs() as f32 * (1.0 + rand)) as u64;
        let wait_time = Duration::from_secs(wait_time);

        ExecutorZkService {
            leader_host_path,
            leader_scheduler_url: None,
            leader_scheduler_changed: Arc::new(AtomicBool::new(true)),
            zk_is_connected: Arc::new(AtomicBool::new(false)),
            zk_client: None,
            zk_session_timeout,
            zk_address,
            listener: None,
            wait_time,
        }
    }

    fn is_zk_connected(&self) -> bool {
        self.zk_is_connected.load(Ordering::SeqCst)
    }

    fn is_leader_scheduler_changed(&self) -> bool {
        self.leader_scheduler_changed.load(Ordering::SeqCst)
    }

    fn change_scheduler(
        &mut self,
        new_scheduler_url: String,
        rt: &Runtime,
    ) -> Result<(), BallistaError> {
        let new_scheduler_url = Some(new_scheduler_url);

        if self.leader_scheduler_url.eq(&new_scheduler_url) {
            info!(
                "The leader scheduler doesn't change, but need to recreate the connection and register to the scheduler again"
            );
        } else {
            info!(
                "Change the leader scheduler from {:?} to the new {:?}",
                &self.leader_scheduler_url, &new_scheduler_url
            );
        }

        self.leader_scheduler_url = new_scheduler_url;
        match self.listener {
            None => {
                warn!("Please set the ExecutorStateChangeListener");
                Err(BallistaError::Internal(
                    "Not set the ExecutorStateChangeListener".to_string(),
                ))
            }
            Some(ref mut inner_listener) => {
                rt.block_on(inner_listener.change_scheduler(
                    self.leader_scheduler_url.as_ref().unwrap().clone(),
                ))
            }
        }
    }

    fn try_to_change_new_scheduler(&mut self, rt: &Runtime) {
        if self.host_path_exist() {
            let scheduler_changed_capture = self.leader_scheduler_changed.clone();
            let watcher = SchedulerChangeWatcher::new(scheduler_changed_capture);

            let zk = self.zk_client.as_ref().unwrap();
            self.leader_scheduler_changed.store(false, Ordering::SeqCst);
            match zk.get_data_w(self.leader_host_path.as_str(), watcher) {
                Ok((data, _)) => {
                    if data.len() == 0 {
                        warn!("The scheduler host path {:?} does not contain any data, and will not change the scheduler", self.leader_host_path);
                    } else {
                        match String::from_utf8(data.clone()) {
                            Ok(new_scheduler_url) => {
                                match self.change_scheduler(new_scheduler_url.clone(), rt)
                                {
                                    Ok(_) => {
                                        info!(
                                            "Change the scheduler to {:?} successfully",
                                            new_scheduler_url
                                        )
                                    }
                                    Err(change_scheduler_error) => {
                                        warn!("Fail to change the scheduler with {:?}, and need to retry", change_scheduler_error);
                                        // if failed to change the scheduler for the executor server, need to retry it.
                                        self.leader_scheduler_changed
                                            .store(true, Ordering::SeqCst);
                                    }
                                }
                            }
                            Err(utf8_error) => {
                                warn!(
                                    "Failed to convert the data {:?} to String with {:?}",
                                    data, utf8_error
                                );
                            }
                        }
                    }
                }
                Err(get_data_error) => {
                    self.leader_scheduler_changed.store(true, Ordering::SeqCst);
                    warn!(
                        "Meet the error {:?}, get the data from scheduler host path",
                        get_data_error
                    );
                }
            }
        } else {
            warn!(
                "The scheduler host path {:?} does not exist",
                self.leader_host_path
            );
        }
    }

    fn host_path_exist(&self) -> bool {
        let zk = self.zk_client.as_ref().unwrap();
        match zk.exists(self.leader_host_path.as_str(), false) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    fn create_zk_connection(&self) -> ZooKeeper {
        loop {
            match ZooKeeper::connect(
                &self.zk_address,
                self.zk_session_timeout.clone(),
                DefaultExecutorZkWatcher,
            ) {
                Ok(zk) => {
                    // try to connect the zk and check the api
                    match zk.exists(&self.leader_host_path, false) {
                        Ok(_) => {
                            info!("Create the zk connection for executor successfully");
                            return zk;
                        }
                        Err(visit_zk_error) => {
                            warn!(
                                "Can't visit the zk with {:?}, the state is {:?}",
                                visit_zk_error, self
                            );
                        }
                    }
                }
                Err(connect_error) => {
                    warn!(
                        "Can't create the zk connection with {:?}, the state is {:?}",
                        connect_error, &self
                    );
                }
            }
            warn!("Wait and retry to create the zk connection");
            self.sleep_wait();
        }
    }

    fn sleep_wait(&self) {
        thread::sleep(self.wait_time);
    }

    fn update_connection_state(&mut self, zk: ZooKeeper) {
        self.zk_client = Some(Arc::new(zk));
        if self.zk_is_connected.swap(true, Ordering::SeqCst) {
            warn!("The connection of zk is online, and don't need to create new zk connection");
        }
    }

    pub fn with_executor_change_listener(
        &mut self,
        listener: Box<dyn ExecutorStateChangeListener>,
    ) {
        self.listener = Some(listener);
    }

    pub fn start_watch_scheduler(
        &mut self,
        mut zk_shutdown: Shutdown,
        zk_complete: mpsc::Sender<()>,
        rt: Runtime,
    ) {
        info!("The executor start watching the change of scheduler");
        loop {
            let is_connected = self.is_zk_connected();
            let scheduler_changed = self.is_leader_scheduler_changed();
            if is_connected {
                if scheduler_changed {
                    self.try_to_change_new_scheduler(&rt);
                    info!("After change scheduler: {:?}", self);
                }
            } else {
                // create the zk connection
                info!("The executor does not connect the zk, and create the connection");
                let zk = self.create_zk_connection();
                self.update_connection_state(zk);

                // add listener to the zk connection and monitor the connection of the network
                let zk = self.zk_client.as_ref().unwrap();
                let is_connected_capture = self.zk_is_connected.clone();
                let is_scheduler_changed_capture = self.leader_scheduler_changed.clone();
                zk.add_listener(move |zk_state| {
                    match zk_state {
                        ZkState::Closed => {
                            info!("The state of zk changed to closed, and the executor should restart to connect the zk");
                            // set this and try to reconnect
                            is_connected_capture.store(false, Ordering::SeqCst);
                            // set this and try to get the latest data info
                            is_scheduler_changed_capture.store(true, Ordering::SeqCst);
                        }
                        other => {
                            info!("The state of zk changed to {:?}", other);
                        }
                    }
                });
                continue;
            }
            self.sleep_wait();

            if zk_shutdown.try_recv() {
                // got shut down message, return the loop
                info!("Stop the scheduler leader watcher service");
                drop(zk_complete);
                return;
            }
        }
    }
}
