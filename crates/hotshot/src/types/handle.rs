//! Provides an event-streaming handle for a [`SystemContext`] running in the background

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_compatibility_layer::art::{async_sleep, async_spawn};
use async_lock::RwLock;
#[cfg(async_executor_impl = "async-std")]
use async_std::task::JoinHandle;
use event_listener::Event as OtherEvent;
use futures::Stream;
use hotshot_task::task::{ConsensusTaskRegistry, NetworkTaskRegistry, Task, TaskState};
use hotshot_task_impls::{events::HotShotEvent, helpers::broadcast_event};
use hotshot_types::{
    consensus::Consensus,
    data::Leaf,
    error::HotShotError,
    traits::{election::Membership, network::ConnectedNetwork, node_implementation::NodeType},
};
#[cfg(async_executor_impl = "tokio")]
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::{traits::NodeImplementation, types::Event, Memberships, SystemContext};

/// A signal for coordinating shutdown across multiple tasks.
///
/// This struct provides methods to initiate a shutdown, check the shutdown status,
/// and wait for a shutdown signal.
pub struct ShutdownSignal {
    event: Arc<OtherEvent>,
    is_shutdown: Arc<AtomicBool>,
}

impl ShutdownSignal {
    /// Creates a new `ShutdownSignal` instance.
    ///
    /// The signal is initialized in a non-shutdown state.
    pub fn new() -> Self {
        Self {
            event: Arc::new(OtherEvent::new()),
            is_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signals a shutdown to all listeners.
    ///
    /// This method sets the shutdown flag and notifies all waiting tasks.
    pub fn signal(&self) {
        self.is_shutdown.store(true, Ordering::SeqCst);
        self.event.notify(usize::MAX); // Notify all listeners
    }

    /// Checks if a shutdown has been signaled.
    ///
    /// Returns `true` if a shutdown has been signaled, `false` otherwise.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    /// Waits for a shutdown signal.
    ///
    /// This method returns immediately if a shutdown has already been signaled.
    /// Otherwise, it waits asynchronously for a shutdown signal.
    pub async fn wait_for_shutdown(&self) {
        if !self.is_shutdown() {
            let listener = self.event.listen();
            if !self.is_shutdown() {
                listener.await;
            }
        }
    }

    /// Creates a new `ShutdownSignal` instance that shares the same underlying state.
    ///
    /// This allows multiple holders to interact with the same shutdown signal.
    pub fn subscribe(&self) -> Self {
        Self {
            event: Arc::clone(&self.event),
            is_shutdown: Arc::clone(&self.is_shutdown),
        }
    }
}

/// Event streaming handle for a [`SystemContext`] instance running in the background
///
/// This type provides the means to message and interact with a background [`SystemContext`] instance,
/// allowing the ability to receive [`Event`]s from it, send transactions to it, and interact with
/// the underlying storage.
pub struct SystemContextHandle<TYPES: NodeType, I: NodeImplementation<TYPES>> {
    /// The [sender](Sender) and [receiver](Receiver),
    /// to allow the application to communicate with HotShot.
    pub(crate) output_event_stream: (Sender<Event<TYPES>>, InactiveReceiver<Event<TYPES>>),

    /// access to the internal event stream, in case we need to, say, shut something down
    #[allow(clippy::type_complexity)]
    pub(crate) internal_event_stream: (
        Sender<Arc<HotShotEvent<TYPES>>>,
        InactiveReceiver<Arc<HotShotEvent<TYPES>>>,
    ),
    /// registry for controlling consensus tasks
    pub(crate) consensus_registry: ConsensusTaskRegistry<HotShotEvent<TYPES>>,

    /// registry for controlling network tasks
    pub(crate) network_registry: NetworkTaskRegistry,

    /// Internal reference to the underlying [`SystemContext`]
    pub hotshot: Arc<SystemContext<TYPES, I>>,

    /// Reference to the internal storage for consensus datum.
    pub(crate) storage: Arc<RwLock<I::Storage>>,

    /// Networks used by the instance of hotshot
    pub network: Arc<I::Network>,

    /// Memberships used by consensus
    pub memberships: Arc<Memberships<TYPES>>,

    /// Shutdown Signal
    pub(crate) shutdown_signal: Arc<ShutdownSignal>,
}

impl<TYPES: NodeType, I: NodeImplementation<TYPES> + 'static> SystemContextHandle<TYPES, I> {
    /// Adds a hotshot consensus-related task to the `SystemContextHandle`.
    pub fn add_task<S: TaskState<Event = HotShotEvent<TYPES>> + 'static>(&mut self, task_state: S) {
        let task = Task::new(
            task_state,
            self.internal_event_stream.0.clone(),
            self.internal_event_stream.1.activate_cloned(),
        );

        self.consensus_registry.run_task(task);
    }

    /// obtains a stream to expose to the user
    pub fn event_stream(&self) -> impl Stream<Item = Event<TYPES>> {
        self.output_event_stream.1.activate_cloned()
    }

    /// HACK so we can know the types when running tests...
    /// there are two cleaner solutions:
    /// - make the stream generic and in nodetypes or nodeimpelmentation
    /// - type wrapper
    #[must_use]
    pub fn event_stream_known_impl(&self) -> Receiver<Event<TYPES>> {
        self.output_event_stream.1.activate_cloned()
    }

    /// HACK so we can create dependency tasks when running tests
    #[must_use]
    pub fn internal_event_stream_sender(&self) -> Sender<Arc<HotShotEvent<TYPES>>> {
        self.internal_event_stream.0.clone()
    }

    /// HACK so we can know the types when running tests...
    /// there are two cleaner solutions:
    /// - make the stream generic and in nodetypes or nodeimpelmentation
    /// - type wrapper
    /// NOTE: this is only used for sanity checks in our tests
    #[must_use]
    pub fn internal_event_stream_receiver_known_impl(&self) -> Receiver<Arc<HotShotEvent<TYPES>>> {
        self.internal_event_stream.1.activate_cloned()
    }

    /// Get the last decided validated state of the [`SystemContext`] instance.
    ///
    /// # Panics
    /// If the internal consensus is in an inconsistent state.
    pub async fn decided_state(&self) -> Arc<TYPES::ValidatedState> {
        self.hotshot.decided_state().await
    }

    /// Get the validated state from a given `view`.
    ///
    /// Returns the requested state, if the [`SystemContext`] is tracking this view. Consensus
    /// tracks views that have not yet been decided but could be in the future. This function may
    /// return [`None`] if the requested view has already been decided (but see
    /// [`decided_state`](Self::decided_state)) or if there is no path for the requested
    /// view to ever be decided.
    pub async fn state(&self, view: TYPES::Time) -> Option<Arc<TYPES::ValidatedState>> {
        self.hotshot.state(view).await
    }

    /// Get the last decided leaf of the [`SystemContext`] instance.
    ///
    /// # Panics
    /// If the internal consensus is in an inconsistent state.
    pub async fn decided_leaf(&self) -> Leaf<TYPES> {
        self.hotshot.decided_leaf().await
    }

    /// Tries to get the most recent decided leaf, returning instantly
    /// if we can't acquire the lock.
    ///
    /// # Panics
    /// Panics if internal consensus is in an inconsistent state.
    #[must_use]
    pub fn try_decided_leaf(&self) -> Option<Leaf<TYPES>> {
        self.hotshot.try_decided_leaf()
    }

    /// Submits a transaction to the backing [`SystemContext`] instance.
    ///
    /// The current node broadcasts the transaction to all nodes on the network.
    ///
    /// # Errors
    ///
    /// Will return a [`HotShotError`] if some error occurs in the underlying
    /// [`SystemContext`] instance.
    pub async fn submit_transaction(
        &self,
        tx: TYPES::Transaction,
    ) -> Result<(), HotShotError<TYPES>> {
        self.hotshot
            .publish_transaction_async(tx, Arc::clone(&self.hotshot.decided_upgrade_certificate))
            .await
    }

    /// Get the underlying consensus state for this [`SystemContext`]
    #[must_use]
    pub fn consensus(&self) -> Arc<RwLock<Consensus<TYPES>>> {
        self.hotshot.consensus()
    }

    /// Shut down the the inner hotshot and wait until all background threads are closed.
    pub async fn shut_down(&mut self) {
        tracing::error!("=== Shutting down node :: {} ===", self.hotshot.id);

        self.shutdown_signal.signal();

        // this is required because `SystemContextHandle` holds an inactive receiver and
        // `broadcast_direct` below can wait indefinitely
        self.internal_event_stream.0.set_await_active(false);
        let _ = self
            .internal_event_stream
            .0
            .broadcast_direct(Arc::new(HotShotEvent::Shutdown))
            .await
            .inspect_err(|err| tracing::error!("Failed to send shutdown event: {err}"));

        tracing::error!("Shutting down the network!");
        self.hotshot.network.shut_down().await;
    
        tracing::error!("Shutting down network tasks!");
        self.network_registry.shutdown().await;

        tracing::error!("Shutting down consensus!");
        self.consensus_registry.shutdown().await;

        tracing::error!("=== Shutting down node :: {} done!!! ===", self.hotshot.id);
    }

    /// return the timeout for a view of the underlying `SystemContext`
    #[must_use]
    pub fn next_view_timeout(&self) -> u64 {
        self.hotshot.next_view_timeout()
    }

    /// Wrapper for `HotShotConsensusApi`'s `leader` function
    #[allow(clippy::unused_async)] // async for API compatibility reasons
    pub async fn leader(&self, view_number: TYPES::Time) -> TYPES::SignatureKey {
        self.hotshot
            .memberships
            .quorum_membership
            .leader(view_number)
    }

    // Below is for testing only:
    /// Wrapper to get this node's public key
    #[cfg(feature = "hotshot-testing")]
    #[must_use]
    pub fn public_key(&self) -> TYPES::SignatureKey {
        self.hotshot.public_key.clone()
    }

    /// Get the sender side of the external event stream for testing purpose
    #[cfg(feature = "hotshot-testing")]
    #[must_use]
    pub fn external_channel_sender(&self) -> Sender<Event<TYPES>> {
        self.output_event_stream.0.clone()
    }

    /// Get the sender side of the internal event stream for testing purpose
    #[cfg(feature = "hotshot-testing")]
    #[must_use]
    pub fn internal_channel_sender(&self) -> Sender<Arc<HotShotEvent<TYPES>>> {
        self.internal_event_stream.0.clone()
    }

    /// Wrapper to get the view number this node is on.
    #[instrument(skip_all, target = "SystemContextHandle", fields(id = self.hotshot.id))]
    pub async fn cur_view(&self) -> TYPES::Time {
        self.hotshot.consensus.read().await.cur_view()
    }

    /// Provides a reference to the underlying storage for this [`SystemContext`], allowing access to
    /// historical data
    #[must_use]
    pub fn storage(&self) -> Arc<RwLock<I::Storage>> {
        Arc::clone(&self.storage)
    }

    /// A helper function to spawn the initial timeout task from a given `SystemContextHandle`.
    #[must_use]
    pub fn spawn_initial_timeout_task(&self) -> JoinHandle<()> {
        // Clone the event stream that we send the timeout event to
        let event_stream = self.internal_event_stream.0.clone();
        let next_view_timeout = self.hotshot.config.next_view_timeout;
        let start_view = self.hotshot.start_view;

        // Spawn a task that will sleep for the next view timeout and then send a timeout event
        // if not cancelled
        async_spawn({
            async move {
                async_sleep(Duration::from_millis(next_view_timeout)).await;
                broadcast_event(
                    Arc::new(HotShotEvent::Timeout(start_view + 1)),
                    &event_stream,
                )
                .await;
            }
        })
    }
}
