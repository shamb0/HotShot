#![allow(clippy::panic)]
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use async_broadcast::{broadcast, Receiver, Sender};
use async_compatibility_layer::art::async_spawn;
use async_lock::RwLock;
#[cfg(async_executor_impl = "async-std")]
use async_std::task::JoinHandle;
use futures::future::join_all;
use hotshot::{
    traits::TestableNodeImplementation,
    types::{Event, SystemContextHandle},
    HotShotInitializer, MarketplaceConfig, Memberships, SystemContext,
};
use hotshot_example_types::{
    auction_results_provider_types::TestAuctionResultsProvider,
    state_types::{TestInstanceState, TestValidatedState},
    storage_types::TestStorage,
};
use hotshot_fakeapi::fake_solver::FakeSolverState;
use hotshot_task_impls::events::HotShotEvent;
use hotshot_types::{
    consensus::ConsensusMetricsValue,
    constants::EVENT_CHANNEL_SIZE,
    data::Leaf,
    simple_certificate::QuorumCertificate,
    traits::{
        election::Membership,
        network::{ConnectedNetwork, Topic},
        node_implementation::{ConsensusTime, NodeImplementation, NodeType},
    },
    HotShotConfig, ValidatorConfig,
};
use tide_disco::Url;
#[cfg(async_executor_impl = "tokio")]
use tokio::task::JoinHandle;
#[allow(deprecated)]
use tracing::info;

use super::{
    completion_task::CompletionTask,
    consistency_task::ConsistencyTask,
    overall_safety_task::{OverallSafetyTask, RoundCtx},
    txn_task::TxnTask,
};
use crate::{
    block_builder::{BuilderTask, TestBuilderImplementation},
    completion_task::CompletionTaskDescription,
    spinning_task::{ChangeNode, SpinningTask, UpDown},
    test_builder::create_test_handle,
    test_launcher::{Network, TestLauncher},
    test_task::{TestResult, TestTask},
    txn_task::TxnTaskDescription,
    view_sync_task::ViewSyncTask,
};

pub trait TaskErr: std::error::Error + Sync + Send + 'static {}
impl<T: std::error::Error + Sync + Send + 'static> TaskErr for T {}

impl<
        TYPES: NodeType<InstanceState = TestInstanceState, ValidatedState = TestValidatedState>,
        I: TestableNodeImplementation<TYPES>,
        N: ConnectedNetwork<TYPES::SignatureKey>,
    > TestRunner<TYPES, I, N>
where
    I: TestableNodeImplementation<TYPES>,
    I: NodeImplementation<
        TYPES,
        Network = N,
        Storage = TestStorage<TYPES>,
        AuctionResultsProvider = TestAuctionResultsProvider<TYPES>,
    >,
{
    /// execute test
    ///
    /// # Panics
    /// if the test fails
    #[allow(clippy::too_many_lines)]
    pub async fn run_test<B: TestBuilderImplementation<TYPES>>(mut self) {
        let (test_sender, test_receiver) = broadcast(EVENT_CHANNEL_SIZE);
        let spinning_changes = self
            .launcher
            .metadata
            .spinning_properties
            .node_changes
            .clone();

        let mut late_start_nodes: HashSet<u64> = HashSet::new();
        let mut restart_nodes: HashSet<u64> = HashSet::new();
        for (_, changes) in &spinning_changes {
            for change in changes {
                if matches!(change.updown, UpDown::Up) {
                    late_start_nodes.insert(change.idx.try_into().unwrap());
                }
                if matches!(change.updown, UpDown::Restart) {
                    restart_nodes.insert(change.idx.try_into().unwrap());
                }
            }
        }

        self.add_nodes::<B>(
            self.launcher.metadata.num_nodes_with_stake,
            &late_start_nodes,
            &restart_nodes,
        )
        .await;
        let mut event_rxs = vec![];
        let mut internal_event_rxs = vec![];

        for node in &self.nodes {
            let r = node.handle.event_stream_known_impl();
            event_rxs.push(r);
        }
        for node in &self.nodes {
            let r = node.handle.internal_event_stream_receiver_known_impl();
            internal_event_rxs.push(r);
        }

        let TestRunner {
            ref launcher,
            nodes,
            solver_server,
            late_start,
            next_node_id: _,
            _pd: _,
        } = self;

        let mut task_futs = vec![];
        let meta = launcher.metadata.clone();

        let handles = Arc::new(RwLock::new(nodes));

        let txn_task =
            if let TxnTaskDescription::RoundRobinTimeBased(duration) = meta.txn_description {
                let txn_task = TxnTask {
                    handles: Arc::clone(&handles),
                    next_node_idx: Some(0),
                    duration,
                    shutdown_chan: test_receiver.clone(),
                };
                Some(txn_task)
            } else {
                None
            };

        // add completion task
        let CompletionTaskDescription::TimeBasedCompletionTaskBuilder(time_based) =
            meta.completion_task_description;
        let completion_task = CompletionTask {
            tx: test_sender.clone(),
            rx: test_receiver.clone(),
            handles: Arc::clone(&handles),
            duration: time_based.duration,
        };

        // add spinning task
        // map spinning to view
        let mut changes: BTreeMap<TYPES::Time, Vec<ChangeNode>> = BTreeMap::new();
        for (view, mut change) in spinning_changes {
            changes
                .entry(TYPES::Time::new(view))
                .or_insert_with(Vec::new)
                .append(&mut change);
        }

        let spinning_task_state = SpinningTask {
            handles: Arc::clone(&handles),
            late_start,
            latest_view: None,
            changes,
            last_decided_leaf: Leaf::genesis(&TestValidatedState::default(), &TestInstanceState {})
                .await,
            high_qc: QuorumCertificate::genesis(
                &TestValidatedState::default(),
                &TestInstanceState {},
            )
            .await,
        };
        let spinning_task = TestTask::<SpinningTask<TYPES, I>>::new(
            spinning_task_state,
            event_rxs.clone(),
            test_receiver.clone(),
        );
        // add safety task
        let overall_safety_task_state = OverallSafetyTask {
            handles: Arc::clone(&handles),
            ctx: RoundCtx::default(),
            properties: self.launcher.metadata.overall_safety_properties.clone(),
            error: None,
            test_sender,
        };

        let consistency_task_state = ConsistencyTask {
            consensus_leaves: BTreeMap::new(),
            safety_properties: self.launcher.metadata.overall_safety_properties,
        };

        let consistency_task = TestTask::<ConsistencyTask<TYPES>>::new(
            consistency_task_state,
            event_rxs.clone(),
            test_receiver.clone(),
        );

        let overall_safety_task = TestTask::<OverallSafetyTask<TYPES, I>>::new(
            overall_safety_task_state,
            event_rxs.clone(),
            test_receiver.clone(),
        );

        // add view sync task
        let view_sync_task_state = ViewSyncTask {
            hit_view_sync: HashSet::new(),
            description: self.launcher.metadata.view_sync_properties,
            _pd: PhantomData,
        };

        let view_sync_task = TestTask::<ViewSyncTask<TYPES, I>>::new(
            view_sync_task_state,
            internal_event_rxs,
            test_receiver.clone(),
        );

        let nodes = handles.read().await;

        // wait for networks to be ready
        for node in &*nodes {
            node.network.wait_for_ready().await;
        }

        // Start hotshot
        for node in &*nodes {
            if !late_start_nodes.contains(&node.node_id) {
                node.handle.hotshot.start_consensus().await;
            }
        }

        drop(nodes);

        task_futs.push(overall_safety_task.run());
        task_futs.push(consistency_task.run());
        task_futs.push(view_sync_task.run());
        task_futs.push(spinning_task.run());

        // `generator` tasks that do not process events.
        let txn_handle = txn_task.map(|txn| txn.run());
        let completion_handle = completion_task.run();

        let mut error_list = vec![];

        #[cfg(async_executor_impl = "async-std")]
        {
            let results = join_all(task_futs).await;
            tracing::error!("test tasks joined");
            for result in results {
                match result {
                    TestResult::Pass => {
                        info!("Task shut down successfully");
                    }
                    TestResult::Fail(e) => error_list.push(e),
                }
            }
            if let Some(handle) = txn_handle {
                handle.cancel().await;
            }
            completion_handle.cancel().await;
        }

        #[cfg(async_executor_impl = "tokio")]
        {
            let results = join_all(task_futs).await;

            tracing::error!("test tasks joined");
            for result in results {
                match result {
                    Ok(res) => match res {
                        TestResult::Pass => {
                            info!("Task shut down successfully");
                        }
                        TestResult::Fail(e) => error_list.push(e),
                    },
                    Err(e) => {
                        tracing::error!("Error Joining the test task {:?}", e);
                    }
                }
            }

            if let Some(handle) = txn_handle {
                handle.abort();
            }
            completion_handle.abort();
        }

        let mut nodes = handles.write().await;

        for node in &mut *nodes {
            node.handle.shut_down().await;
        }

        // Shutdown all of the servers at the end
        // Aborting here doesn't cause any problems because we don't maintain any state
        if let Some(solver_server) = solver_server {
            #[cfg(async_executor_impl = "async-std")]
            solver_server.1.cancel().await;
            #[cfg(async_executor_impl = "tokio")]
            solver_server.1.abort();
        }

        assert!(
            error_list.is_empty(),
            "{}",
            error_list
                .iter()
                .fold("TEST FAILED! Results:".to_string(), |acc, error| {
                    format!("{acc}\n\n{error:?}")
                })
        );
    }

    pub async fn init_builders<B: TestBuilderImplementation<TYPES>>(
        &self,
    ) -> (Vec<Box<dyn BuilderTask<TYPES>>>, Vec<Url>) {
        let config = self.launcher.resource_generator.config.clone();
        let mut builder_tasks = Vec::new();
        let mut builder_urls = Vec::new();
        for metadata in &self.launcher.metadata.builders {
            let builder_port = portpicker::pick_unused_port().expect("No free ports");
            let builder_url =
                Url::parse(&format!("http://localhost:{builder_port}")).expect("Valid URL");
            let builder_task = B::start(
                config.num_nodes_with_stake.into(),
                builder_url.clone(),
                B::Config::default(),
                metadata.changes.clone(),
            )
            .await;
            builder_tasks.push(builder_task);
            builder_urls.push(builder_url);
        }

        (builder_tasks, builder_urls)
    }

    /// Add servers.
    pub async fn add_servers(&mut self, builder_urls: Vec<Url>) {
        let solver_error_pct = self.launcher.metadata.solver.error_pct;
        let solver_port = portpicker::pick_unused_port().expect("No available ports");

        // This should basically never fail
        let solver_url: Url = format!("http://localhost:{solver_port}")
            .parse()
            .expect("Failed to parse solver URL");

        // Initialize the solver API state
        let solver_state = FakeSolverState::new(Some(solver_error_pct), builder_urls);

        // Then, fire it up as a background thread.
        self.solver_server = Some((
            solver_url.clone(),
            async_spawn(async move {
                solver_state
                    .run::<TYPES>(solver_url)
                    .await
                    .expect("Unable to run solver api");
            }),
        ));
    }

    /// Add nodes.
    ///
    /// # Panics
    /// Panics if unable to create a [`HotShotInitializer`]
    pub async fn add_nodes<B: TestBuilderImplementation<TYPES>>(
        &mut self,
        total: usize,
        late_start: &HashSet<u64>,
        restart: &HashSet<u64>,
    ) -> Vec<u64> {
        let mut results = vec![];
        let config = self.launcher.resource_generator.config.clone();
        let known_nodes_with_stake = config.known_nodes_with_stake.clone();

        let (mut builder_tasks, builder_urls) = self.init_builders::<B>().await;
        self.add_servers(builder_urls.clone()).await;

        // Collect uninitialized nodes because we need to wait for all networks to be ready before starting the tasks
        let mut uninitialized_nodes = Vec::new();
        let mut networks_ready = Vec::new();

        for i in 0..total {
            let mut config = config.clone();
            let node_id = self.next_node_id;
            self.next_node_id += 1;
            tracing::debug!("launch node {}", i);

            let memberships = Memberships {
                quorum_membership: <TYPES as NodeType>::Membership::create_election(
                    known_nodes_with_stake.clone(),
                    known_nodes_with_stake.clone(),
                    Topic::Global,
                    config.fixed_leader_for_gpuvid,
                ),
                da_membership: <TYPES as NodeType>::Membership::create_election(
                    known_nodes_with_stake.clone(),
                    config.known_da_nodes.clone(),
                    Topic::Da,
                    config.fixed_leader_for_gpuvid,
                ),
                vid_membership: <TYPES as NodeType>::Membership::create_election(
                    known_nodes_with_stake.clone(),
                    known_nodes_with_stake.clone(),
                    Topic::Global,
                    config.fixed_leader_for_gpuvid,
                ),
                view_sync_membership: <TYPES as NodeType>::Membership::create_election(
                    known_nodes_with_stake.clone(),
                    known_nodes_with_stake.clone(),
                    Topic::Global,
                    config.fixed_leader_for_gpuvid,
                ),
            };
            config.builder_urls = builder_urls
                .clone()
                .try_into()
                .expect("Non-empty by construction");

            let network = (self.launcher.resource_generator.channel_generator)(node_id).await;
            let storage = (self.launcher.resource_generator.storage)(node_id);
            let mut marketplace_config =
                (self.launcher.resource_generator.marketplace_config)(node_id);
            if let Some(solver_server) = &self.solver_server {
                let mut new_auction_results_provider =
                    marketplace_config.auction_results_provider.as_ref().clone();

                new_auction_results_provider.broadcast_url = Some(solver_server.0.clone());

                marketplace_config.auction_results_provider = new_auction_results_provider.into();
            }

            let network_clone = network.clone();
            let networks_ready_future = async move {
                network_clone.wait_for_ready().await;
            };

            networks_ready.push(networks_ready_future);

            if late_start.contains(&node_id) {
                if self.launcher.metadata.skip_late {
                    self.late_start.insert(
                        node_id,
                        LateStartNode {
                            network,
                            context: LateNodeContext::UninitializedContext(
                                LateNodeContextParameters {
                                    storage,
                                    memberships,
                                    config,
                                    marketplace_config,
                                },
                            ),
                        },
                    );
                } else {
                    let initializer =
                        HotShotInitializer::<TYPES>::from_genesis(TestInstanceState {})
                            .await
                            .unwrap();

                    // See whether or not we should be DA
                    let is_da = node_id < config.da_staked_committee_size as u64;

                    // We assign node's public key and stake value rather than read from config file since it's a test
                    let validator_config =
                        ValidatorConfig::generated_from_seed_indexed([0u8; 32], node_id, 1, is_da);
                    let hotshot = Self::add_node_with_config(
                        node_id,
                        network.clone(),
                        memberships,
                        initializer,
                        config,
                        validator_config,
                        storage,
                        marketplace_config,
                    )
                    .await;
                    self.late_start.insert(
                        node_id,
                        LateStartNode {
                            network,
                            context: LateNodeContext::InitializedContext(hotshot),
                        },
                    );
                }
            } else {
                uninitialized_nodes.push((
                    node_id,
                    network,
                    memberships,
                    config,
                    storage,
                    marketplace_config,
                ));
            }

            results.push(node_id);
        }

        // Add the restart nodes after the rest.  This must be done after all the original networks are
        // created because this will reset the bootstrap info for the restarted nodes
        for node_id in &results {
            if restart.contains(node_id) {
                self.late_start.insert(
                    *node_id,
                    LateStartNode {
                        network: (self.launcher.resource_generator.channel_generator)(*node_id)
                            .await,
                        context: LateNodeContext::Restart,
                    },
                );
            }
        }

        // Wait for all networks to be ready
        join_all(networks_ready).await;

        // Then start the necessary tasks
        for (node_id, network, memberships, config, storage, marketplace_config) in
            uninitialized_nodes
        {
            let behaviour = (self.launcher.metadata.behaviour)(node_id);
            let handle = create_test_handle(
                behaviour,
                node_id,
                network.clone(),
                memberships,
                config.clone(),
                storage,
                marketplace_config,
            )
            .await;

            match node_id.cmp(&(config.da_staked_committee_size as u64 - 1)) {
                std::cmp::Ordering::Less => {
                    if let Some(task) = builder_tasks.pop() {
                        task.start(Box::new(handle.event_stream()))
                    }
                }
                std::cmp::Ordering::Equal => {
                    // If we have more builder tasks than DA nodes, pin them all on the last node.
                    while let Some(task) = builder_tasks.pop() {
                        task.start(Box::new(handle.event_stream()))
                    }
                }
                std::cmp::Ordering::Greater => {}
            }

            self.nodes.push(Node {
                node_id,
                network,
                handle,
            });
        }

        results
    }

    /// add a specific node with a config
    /// # Panics
    /// if unable to initialize the node's `SystemContext` based on the config
    #[allow(clippy::too_many_arguments)]
    pub async fn add_node_with_config(
        node_id: u64,
        network: Network<TYPES, I>,
        memberships: Memberships<TYPES>,
        initializer: HotShotInitializer<TYPES>,
        config: HotShotConfig<TYPES::SignatureKey>,
        validator_config: ValidatorConfig<TYPES::SignatureKey>,
        storage: I::Storage,
        marketplace_config: MarketplaceConfig<TYPES, I>,
    ) -> Arc<SystemContext<TYPES, I>> {
        // Get key pair for certificate aggregation
        let private_key = validator_config.private_key.clone();
        let public_key = validator_config.public_key.clone();

        SystemContext::new(
            public_key,
            private_key,
            node_id,
            config,
            memberships,
            network,
            initializer,
            ConsensusMetricsValue::default(),
            storage,
            marketplace_config,
        )
    }

    /// add a specific node with a config
    /// # Panics
    /// if unable to initialize the node's `SystemContext` based on the config
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub async fn add_node_with_config_and_channels(
        node_id: u64,
        network: Network<TYPES, I>,
        memberships: Memberships<TYPES>,
        initializer: HotShotInitializer<TYPES>,
        config: HotShotConfig<TYPES::SignatureKey>,
        validator_config: ValidatorConfig<TYPES::SignatureKey>,
        storage: I::Storage,
        marketplace_config: MarketplaceConfig<TYPES, I>,
        internal_channel: (
            Sender<Arc<HotShotEvent<TYPES>>>,
            Receiver<Arc<HotShotEvent<TYPES>>>,
        ),
        external_channel: (Sender<Event<TYPES>>, Receiver<Event<TYPES>>),
    ) -> Arc<SystemContext<TYPES, I>> {
        // Get key pair for certificate aggregation
        let private_key = validator_config.private_key.clone();
        let public_key = validator_config.public_key.clone();

        SystemContext::new_from_channels(
            public_key,
            private_key,
            node_id,
            config,
            memberships,
            network,
            initializer,
            ConsensusMetricsValue::default(),
            storage,
            marketplace_config,
            internal_channel,
            external_channel,
        )
    }
}

/// a node participating in a test
pub struct Node<TYPES: NodeType, I: TestableNodeImplementation<TYPES>> {
    /// The node's unique identifier
    pub node_id: u64,
    /// The underlying network belonging to the node
    pub network: Network<TYPES, I>,
    /// The handle to the node's internals
    pub handle: SystemContextHandle<TYPES, I>,
}

/// This type combines all of the paramters needed to build the context for a node that started
/// late during a unit test or integration test.
pub struct LateNodeContextParameters<TYPES: NodeType, I: TestableNodeImplementation<TYPES>> {
    /// The storage trait for Sequencer persistence.
    pub storage: I::Storage,

    /// The memberships of this particular node.
    pub memberships: Memberships<TYPES>,

    /// The config associted with this node.
    pub config: HotShotConfig<TYPES::SignatureKey>,

    /// The marketplace config for this node.
    pub marketplace_config: MarketplaceConfig<TYPES, I>,
}

/// The late node context dictates how we're building a node that started late during the test.
#[allow(clippy::large_enum_variant)]
pub enum LateNodeContext<TYPES: NodeType, I: TestableNodeImplementation<TYPES>> {
    /// The system context that we're passing directly to the node, this means the node is already
    /// initialized successfully.
    InitializedContext(Arc<SystemContext<TYPES, I>>),

    /// The system context that we're passing to the node when it is not yet initialized, so we're
    /// initializing it based on the received leaf and init parameters.
    UninitializedContext(LateNodeContextParameters<TYPES, I>),
    /// The node is to be restarted so we will build the context from the node that was already running.
    Restart,
}

/// A yet-to-be-started node that participates in tests
pub struct LateStartNode<TYPES: NodeType, I: TestableNodeImplementation<TYPES>> {
    /// The underlying network belonging to the node
    pub network: Network<TYPES, I>,
    /// Either the context to which we will use to launch HotShot for initialized node when it's
    /// time, or the parameters that will be used to initialize the node and launch HotShot.
    pub context: LateNodeContext<TYPES, I>,
}

/// The runner of a test network
/// spin up and down nodes, execute rounds
pub struct TestRunner<
    TYPES: NodeType,
    I: TestableNodeImplementation<TYPES>,
    N: ConnectedNetwork<TYPES::SignatureKey>,
> {
    /// test launcher, contains a bunch of useful metadata and closures
    pub(crate) launcher: TestLauncher<TYPES, I>,
    /// nodes in the test
    pub(crate) nodes: Vec<Node<TYPES, I>>,
    /// the solver server running in the test
    pub(crate) solver_server: Option<(Url, JoinHandle<()>)>,
    /// nodes with a late start
    pub(crate) late_start: HashMap<u64, LateStartNode<TYPES, I>>,
    /// the next node unique identifier
    pub(crate) next_node_id: u64,
    /// Phantom for N
    pub(crate) _pd: PhantomData<N>,
}
