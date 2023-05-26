use crate::scheduler_server::SchedulerServer;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{info, warn};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{iter, thread};
use tokio::runtime::Runtime;
use zookeeper::recipes::leader::LeaderLatch;
use zookeeper::{
    Acl, CreateMode, WatchedEvent, Watcher, ZkError, ZkResult, ZkState, ZooKeeper,
};

#[tonic::async_trait]
pub trait SchedulerStateChangeListener: Send + Sync {
    /// The scheduler changed from leader to follower,
    /// the leader election service call this function and make all listener to be a follower state
    async fn change_to_follower(&self);
    /// The scheduler changed from follower to leader,
    /// the leader election service call this function and make all listener to be a leader state
    async fn change_to_leader(&self);
}

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    SchedulerStateChangeListener for SchedulerServer<T, U>
{
    async fn change_to_follower(&self) {
        info!("Prepare to change to the follower: stop the scheduler service");
        self.stop_service().await;
    }

    async fn change_to_leader(&self) {
        info!("Prepare to change to the leader: restart the scheduler service");
        self.restart_service().await;
    }
}

pub struct DefaultSchedulerZkWatcher;

impl Watcher for DefaultSchedulerZkWatcher {
    fn handle(&self, event: WatchedEvent) {
        info!("Default SchedulerZkWatcher: {:?}", event);
    }
}

pub struct SchedulerZkLeaderService {
    leader_host_path: String,
    leader_election_path: String,
    current_scheduler_address: String,
    id: Option<String>,
    is_leader: Arc<AtomicBool>,
    zk_is_connected: Arc<AtomicBool>,
    zk_client: Option<Arc<ZooKeeper>>,
    zk_leader_latch: Option<Arc<LeaderLatch>>,
    // the timeout for zookeeper client
    zk_session_timeout: Duration,
    zk_address: String,
    listener: Box<dyn SchedulerStateChangeListener>,
    wait_time: Duration,
}

impl Debug for SchedulerZkLeaderService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let role = match self.is_leader() {
            true => "zk_leader",
            false => "zk_follower",
        };
        f.debug_struct("SchedulerZkLeaderService")
            .field("id", &self.id)
            .field("role", &role)
            .field("current_host", &self.current_scheduler_address)
            .field("zk_is_connected", &self.is_zk_connected())
            .field("zk_session_timeout", &self.zk_session_timeout)
            .field("wait_time", &self.wait_time)
            .finish()
    }
}

impl SchedulerZkLeaderService {
    pub fn new(
        leader_host_path: String,
        leader_election_path: String,
        current_scheduler_address: String,
        zk_session_timeout: Duration,
        zk_address: String,
        listener: Box<dyn SchedulerStateChangeListener>,
    ) -> Self {
        let rand = rand::random::<f32>();
        let wait_time = (zk_session_timeout.as_secs() as f32 * (1.0 + rand)) as u64;
        let wait_time = Duration::from_secs(wait_time);

        let service = SchedulerZkLeaderService {
            leader_host_path,
            leader_election_path,
            current_scheduler_address,
            id: None,
            is_leader: Arc::new(AtomicBool::from(false)),
            zk_is_connected: Arc::new(AtomicBool::from(false)),
            zk_client: None,
            zk_leader_latch: None,
            zk_session_timeout,
            zk_address,
            listener,
            wait_time,
        };
        service
    }

    fn sleep_wait(&self) {
        thread::sleep(self.wait_time);
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    fn is_zk_connected(&self) -> bool {
        self.zk_is_connected.load(Ordering::SeqCst)
    }

    fn ensure_path(zk: &ZooKeeper, path: &str) -> ZkResult<()> {
        for (i, _) in path
            .chars()
            .chain(iter::once('/'))
            .enumerate()
            .skip(1)
            .filter(|c| c.1 == '/')
        {
            match zk.create(
                &path[..i],
                vec![],
                Acl::open_unsafe().clone(),
                CreateMode::Container,
            ) {
                Ok(_) | Err(ZkError::NodeExists) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn change_to_leader(&self, rt: &Runtime) {
        info!("Begin to change scheduler to leader");
        // change state of the listener
        rt.block_on(self.listener.change_to_leader());

        let zk = self.zk_client.as_ref().unwrap();
        let data = self.current_scheduler_address.as_bytes().to_vec();

        // make sure the `leader_host_path` exist.
        match zk.exists(&self.leader_host_path, false) {
            Ok(None) => match Self::ensure_path(zk, &self.leader_host_path) {
                Ok(_) => {}
                Err(create_path_error) => {
                    warn!(
                        "Failed to ensure path {:?} with {:?}",
                        self.leader_host_path, create_path_error
                    );
                    return;
                }
            },
            Err(exists_error) => {
                warn!(
                    "Failed to check zk path exist with {:?}, the path {:?}",
                    exists_error, self.leader_host_path
                );
                return;
            }
            _ => {}
        }

        // make sure the data is set
        match zk.set_data(&self.leader_host_path, data, None) {
            Ok(_) => {
                info!(
                    "Set data [{:?}] to the leader host path [{:?}] successfully",
                    self.current_scheduler_address, self.leader_host_path,
                );
            }
            Err(set_data_error) => {
                warn!(
                    "Failed to set data [{:?}] to the leader host path [{:?}] with {:?}",
                    self.current_scheduler_address, self.leader_host_path, set_data_error,
                );
                return;
            }
        }

        // from `false` to `true`
        info!(
            "The scheduler {:?} changed from follower to leader with new id {:?}",
            self.current_scheduler_address, self.id
        );
        self.is_leader.store(true, Ordering::SeqCst);
    }

    fn change_to_follower(&self, rt: &Runtime) {
        info!("Begin to change scheduler to follower");
        // change state of the listener
        rt.block_on(self.listener.change_to_follower());
        // from `true` to `false`
        info!(
            "The scheduler {:?} changed from leader to follower with id {:?}",
            self.current_scheduler_address, self.id
        );
        self.is_leader.store(false, Ordering::SeqCst);
    }

    fn create_zk_connection(&self) -> Arc<ZooKeeper> {
        loop {
            match ZooKeeper::connect(
                &self.zk_address,
                self.zk_session_timeout.clone(),
                DefaultSchedulerZkWatcher,
            ) {
                Ok(zk) => {
                    // try to connect the zk and check the api availability
                    match zk.exists(&self.leader_election_path, false) {
                        Ok(_) => {
                            info!("Create the zk connection for scheduler successfully");
                            return Arc::new(zk);
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
                        connect_error, self
                    );
                }
            }
            warn!("Wait and retry to create the zk connection");
            self.sleep_wait();
        }
    }

    fn create_leader_latch(&mut self, zk: Arc<ZooKeeper>) -> bool {
        let id = self.current_scheduler_address.clone()
            + "_"
            + SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string()
                .as_str();
        let leader_latch = Arc::new(LeaderLatch::new(
            zk.clone(),
            id.clone(),
            self.leader_election_path.clone(),
        ));
        return match leader_latch.start() {
            Ok(_) => {
                info!("The scheduler {:?} begin the new leader election successfully with new id {:?}, previous id {:?}", self.current_scheduler_address, id, self.id);
                self.update_zk_connection(zk, leader_latch, id);
                self.change_connection_state(true);
                true
            }
            Err(_) => {
                warn!("The scheduler {:?} failed to the new leader election with new id {:?}, previous id {:?}, and will retry", self.current_scheduler_address, id, self.id);
                false
            }
        };
    }

    fn connect_and_leader_election(&mut self) {
        // make sure both the connection of zk and leader election are ready.
        // if one of them is not ready, need to retry it until success.
        let zk = self.create_zk_connection();
        let mut start_election = self.create_leader_latch(zk);
        while !start_election {
            self.sleep_wait();
            let zk = self.create_zk_connection();
            start_election = self.create_leader_latch(zk);
        }

        // add listener to listen the changes of zk status
        let zk = self.zk_client.as_ref().unwrap();
        let is_connected_capture = self.zk_is_connected.clone();
        let id_capture = self.id.clone();
        let scheduler_address_capture = self.current_scheduler_address.clone();
        zk.add_listener(move |zk_state| {
            match zk_state {
                ZkState::Closed => {
                    // the connection of zk has been closed
                    // and need to create zk and leader election again
                    info!("The state of zk changed to closed, the scheduler {:?} should restart the election with id {:?}",scheduler_address_capture, id_capture);
                    is_connected_capture.store(false, Ordering::SeqCst);
                }
                other => {
                    info!("The state of zk changed to {:?}, the scheduler {:?} with id {:?}", other, scheduler_address_capture, id_capture);
                }
            };
        });
    }

    fn change_connection_state(&self, is_connect: bool) {
        self.zk_is_connected.store(is_connect, Ordering::SeqCst);
    }

    fn update_zk_connection(
        &mut self,
        zk: Arc<ZooKeeper>,
        leader_latch: Arc<LeaderLatch>,
        id: String,
    ) {
        self.zk_client = Some(zk);
        self.zk_leader_latch = Some(leader_latch);
        self.id = Some(id);
    }

    pub fn start_election(&mut self, rt: Runtime) {
        info!(
            "The scheduler {} start the leader election",
            self.current_scheduler_address
        );
        // change to follower first
        self.change_to_follower(&rt);

        loop {
            let is_connected = self.is_zk_connected();
            if !is_connected {
                // this scheduler is not connected to the zk
                self.connect_and_leader_election();
                // to the next loop quickly
                continue;
            } else {
                let leader_latch = self.zk_leader_latch.as_ref().unwrap();
                match (self.is_leader(), leader_latch.has_leadership()) {
                    (true, false) => {
                        // leader -> follower
                        self.change_to_follower(&rt);
                    }
                    (false, true) => {
                        // follower -> leader
                        self.change_to_leader(&rt);
                    }
                    _ => {}
                }
            }
            self.sleep_wait();
        }
    }
}
