use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rdkafka::{
    admin::AdminClient, client::ClientContext, config::RDKafkaLogLevel, consumer::ConsumerContext,
    error::KafkaError,
};
use tokio::{
    sync::{Mutex, MutexGuard, oneshot, watch},
    time::Instant,
};

use crate::{
    domain::cluster::DecryptedClusterConfig,
    kafka::{
        client::{ConnectionError, ConnectionTestResult},
        config::build_client_properties,
        group_consumers::GroupConsumerCache,
    },
};

pub const LEASE_DURATION: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    #[error("Monitoring session expired; create a new session")]
    Expired,
    #[error("A snapshot is already being collected")]
    CollectionInProgress,
    #[error("The application is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Connection(#[from] ConnectionError),
}

pub trait SessionClientFactory: Clone + Send + Sync + 'static {
    type Client: Send + 'static;
    fn create(
        &self,
        config: DecryptedClusterConfig,
    ) -> impl Future<Output = Result<Self::Client, SessionError>> + Send;

    fn test_connection(
        &self,
        config: DecryptedClusterConfig,
    ) -> impl Future<Output = Result<ConnectionTestResult, SessionError>> + Send {
        async {
            crate::kafka::client::test_connection(config)
                .await
                .map_err(SessionError::from)
        }
    }

    fn snapshot(
        &self,
        _client: &mut Self::Client,
        _cluster_id: &str,
    ) -> impl Future<Output = Result<crate::domain::snapshot::ClusterSnapshot, SessionError>> + Send
    {
        async { Err(SessionError::Connection(ConnectionError::InvalidConfig)) }
    }
}

/// Suppress native messages, which may contain decrypted credentials.
pub struct SessionContext;
impl ClientContext for SessionContext {
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, _: KafkaError, _: &str) {}
    fn stats_raw(&self, _: &[u8]) {}
}
impl ConsumerContext for SessionContext {}

pub struct KafkaSessionClient {
    pub admin: Arc<AdminClient<SessionContext>>,
    // Populated lazily by snapshot collection, never subscribed or auto-committing.
    pub group_consumers: GroupConsumerCache,
    pub config: DecryptedClusterConfig,
}

#[derive(Clone)]
pub struct KafkaSessionClientFactory;

impl SessionClientFactory for KafkaSessionClientFactory {
    type Client = KafkaSessionClient;

    async fn create(&self, config: DecryptedClusterConfig) -> Result<Self::Client, SessionError> {
        tokio::task::spawn_blocking(move || {
            let properties =
                build_client_properties(&config).map_err(|_| ConnectionError::InvalidConfig)?;
            let admin = properties
                .inner
                .create_with_context(SessionContext)
                .map_err(|_| ConnectionError::InvalidConfig)?;
            Ok(KafkaSessionClient {
                admin: Arc::new(admin),
                group_consumers: GroupConsumerCache::default(),
                config,
            })
        })
        .await
        .map_err(|_| ConnectionError::Timeout)?
    }

    async fn snapshot(
        &self,
        client: &mut Self::Client,
        cluster_id: &str,
    ) -> Result<crate::domain::snapshot::ClusterSnapshot, SessionError> {
        use crate::kafka::collector::{CollectionOptions, NativeProbe, collect};
        let options = CollectionOptions {
            operation_timeout: client.config.connection_timeout,
            ..CollectionOptions::default()
        };
        let probe = NativeProbe::new(client, options);
        let snapshot = collect(&probe, cluster_id, options).await;
        // Timed-out native calls cannot be cancelled: retain session locks until they exit.
        probe.finish().await;
        Ok(snapshot)
    }
}

pub struct SessionEntry<C> {
    pub cluster_id: String,
    /// Held throughout collection; closing takes the resource even if an Arc handle survives.
    pub clients: Mutex<Option<C>>,
    collection_lock: Mutex<()>,
    closed: AtomicBool,
}

impl<C: Send + 'static> SessionEntry<C> {
    pub fn try_collect(&self) -> Result<MutexGuard<'_, ()>, SessionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SessionError::Expired);
        }
        let guard = self
            .collection_lock
            .try_lock()
            .map_err(|_| SessionError::CollectionInProgress)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(SessionError::Expired);
        }
        Ok(guard)
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let clients = self.clients.lock().await.take();
        // Native client destruction can block while its poll threads terminate.
        if let Some(clients) = clients {
            let _ = tokio::task::spawn_blocking(move || drop(clients)).await;
        }
    }
}

struct LeasedEntry<C> {
    session: Arc<SessionEntry<C>>,
    renewed_at: Instant,
}

struct Sessions<C> {
    entries: HashMap<String, LeasedEntry<C>>,
    operations: HashMap<String, Operation>,
    shutting_down: bool,
}

/// A completion record lives in the manager until its owned task has released resources.
struct Operation {
    cluster_id: Option<String>,
    closing_session_id: Option<String>,
    expired: bool,
    cancelled: Arc<AtomicBool>,
    completion: watch::Receiver<bool>,
}

impl<C> Sessions<C> {
    fn register_operation(
        &mut self,
        cluster_id: Option<String>,
        closing_session_id: Option<String>,
        expired: bool,
    ) -> (
        String,
        watch::Sender<bool>,
        watch::Receiver<bool>,
        Arc<AtomicBool>,
    ) {
        let id = uuid::Uuid::new_v4().to_string();
        let (done, completion) = watch::channel(false);
        let cancelled = Arc::new(AtomicBool::new(false));
        self.operations.insert(
            id.clone(),
            Operation {
                cluster_id,
                closing_session_id,
                expired,
                cancelled: cancelled.clone(),
                completion: completion.clone(),
            },
        );
        (id, done, completion, cancelled)
    }
}

pub struct SessionManager<F: SessionClientFactory = KafkaSessionClientFactory> {
    factory: F,
    lease: Duration,
    state: Arc<Mutex<Sessions<F::Client>>>,
    cluster_gate: Arc<Mutex<()>>,
}

impl<F: SessionClientFactory> Clone for SessionManager<F> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            lease: self.lease,
            state: self.state.clone(),
            cluster_gate: self.cluster_gate.clone(),
        }
    }
}

impl<F: SessionClientFactory> SessionManager<F> {
    pub async fn test_connection(
        &self,
        cluster_id: Option<String>,
        config: DecryptedClusterConfig,
    ) -> Result<ConnectionTestResult, SessionError> {
        let mut state = self.state.lock().await;
        if state.shutting_down {
            return Err(SessionError::ShuttingDown);
        }
        let (operation_id, done, _, _) = state.register_operation(cluster_id, None, false);
        let shared = self.state.clone();
        let factory = self.factory.clone();
        let (reply, result) = oneshot::channel();
        // The manager owns this task, including native work and destructor completion.
        // Dropping the HTTP response receiver never drops the factory future.
        tokio::spawn(async move {
            let outcome = factory.test_connection(config).await;
            shared.lock().await.operations.remove(&operation_id);
            done.send_replace(true);
            let _ = reply.send(outcome);
        });
        drop(state);
        result
            .await
            .map_err(|_| SessionError::Connection(ConnectionError::Timeout))?
    }

    pub async fn snapshot(
        &self,
        id: &str,
    ) -> Result<crate::domain::snapshot::ClusterSnapshot, SessionError> {
        let session = self.get_and_renew(id).await?;
        let factory = self.factory.clone();
        // The owned task retains the collection/client locks even if HTTP is cancelled.
        tokio::spawn(async move {
            let _collection = session.try_collect()?;
            let mut clients = session.clients.lock().await;
            let client = clients.as_mut().ok_or(SessionError::Expired)?;
            factory.snapshot(client, &session.cluster_id).await
        })
        .await
        .map_err(|_| SessionError::Connection(ConnectionError::Timeout))?
    }

    pub fn new(lease: Duration, factory: F) -> Self {
        let state = Arc::new(Mutex::new(Sessions {
            entries: HashMap::<String, LeasedEntry<F::Client>>::new(),
            operations: HashMap::new(),
            shutting_down: false,
        }));
        let weak = Arc::downgrade(&state);
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let Some(state) = weak.upgrade() else {
                    break;
                };
                if state.lock().await.shutting_down {
                    break;
                }
                Self::reap(&state, lease).await;
            }
        });
        Self {
            factory,
            lease,
            state,
            cluster_gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn lease_seconds(&self) -> u64 {
        self.lease.as_secs()
    }

    /// Read correlation metadata without renewing leases or taking ownership of clients.
    pub async fn cluster_id(&self, id: &str) -> Option<String> {
        let state = self.state.lock().await;
        state
            .entries
            .get(id)
            .map(|entry| entry.session.cluster_id.clone())
            .or_else(|| {
                state
                    .operations
                    .values()
                    .find(|operation| operation.closing_session_id.as_deref() == Some(id))
                    .and_then(|operation| operation.cluster_id.clone())
            })
    }

    /// Serializes API cluster lookup/create with close/delete to prevent orphan sessions.
    pub async fn cluster_operation(&self) -> MutexGuard<'_, ()> {
        self.cluster_gate.lock().await
    }

    pub async fn create(
        &self,
        cluster_id: impl Into<String>,
        config: DecryptedClusterConfig,
    ) -> Result<String, SessionError> {
        let cluster_id = cluster_id.into();
        let mut state = self.state.lock().await;
        if state.shutting_down {
            return Err(SessionError::ShuttingDown);
        }
        let (operation_id, done, _, cancelled) =
            state.register_operation(Some(cluster_id.clone()), None, false);
        let (reply, result) = oneshot::channel();
        let shared = self.state.clone();
        let factory = self.factory.clone();
        // Registration and spawn happen under the same lock, with no cancellation point.
        // Cancelling this request drops only the reply receiver, never the owning task.
        tokio::spawn(async move {
            match factory.create(config).await {
                Ok(client) => {
                    let session = Arc::new(SessionEntry {
                        cluster_id,
                        clients: Mutex::new(Some(client)),
                        collection_lock: Mutex::new(()),
                        closed: AtomicBool::new(false),
                    });
                    let mut state = shared.lock().await;
                    if state.shutting_down || cancelled.load(Ordering::Acquire) || reply.is_closed()
                    {
                        let error = if state.shutting_down {
                            SessionError::ShuttingDown
                        } else {
                            SessionError::Expired
                        };
                        drop(state);
                        session.close().await;
                        let _ = reply.send(Err(error));
                    } else {
                        let id = uuid::Uuid::new_v4().to_string();
                        state.entries.insert(
                            id.clone(),
                            LeasedEntry {
                                session: session.clone(),
                                renewed_at: Instant::now(),
                            },
                        );
                        if reply.send(Ok(id.clone())).is_err() {
                            state.entries.remove(&id);
                            drop(state);
                            session.close().await;
                        }
                    }
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            }
            shared.lock().await.operations.remove(&operation_id);
            done.send_replace(true);
        });
        drop(state);
        result
            .await
            .map_err(|_| SessionError::Connection(ConnectionError::Timeout))?
    }

    pub async fn get(&self, id: &str) -> Result<Arc<SessionEntry<F::Client>>, SessionError> {
        self.lookup(id, false).await
    }

    pub async fn get_and_renew(
        &self,
        id: &str,
    ) -> Result<Arc<SessionEntry<F::Client>>, SessionError> {
        self.lookup(id, true).await
    }

    async fn lookup(
        &self,
        id: &str,
        renew: bool,
    ) -> Result<Arc<SessionEntry<F::Client>>, SessionError> {
        let mut state = self.state.lock().await;
        let Some(entry) = state.entries.get_mut(id) else {
            let closing = Self::pending_close(&state, id);
            drop(state);
            if let Some(completion) = closing {
                Self::wait(completion).await;
            }
            return Err(SessionError::Expired);
        };
        let now = Instant::now();
        if now.duration_since(entry.renewed_at) >= self.lease {
            let expired = state.entries.remove(id).expect("entry exists").session;
            let completion =
                Self::start_cleanup(&self.state, &mut state, id.to_owned(), expired, true);
            drop(state);
            Self::wait(completion).await;
            return Err(SessionError::Expired);
        }
        if renew {
            entry.renewed_at = now;
        }
        Ok(entry.session.clone())
    }

    pub async fn close(&self, id: &str) -> Result<(), SessionError> {
        let mut state = self.state.lock().await;
        let (completion, expired) = if let Some(entry) = state.entries.remove(id) {
            let expired = Instant::now().duration_since(entry.renewed_at) >= self.lease;
            (
                Self::start_cleanup(
                    &self.state,
                    &mut state,
                    id.to_owned(),
                    entry.session,
                    expired,
                ),
                expired,
            )
        } else {
            let operation = state
                .operations
                .values()
                .find(|operation| operation.closing_session_id.as_deref() == Some(id))
                .ok_or(SessionError::Expired)?;
            (operation.completion.clone(), operation.expired)
        };
        drop(state);
        Self::wait(completion).await;
        if expired {
            Err(SessionError::Expired)
        } else {
            Ok(())
        }
    }

    pub async fn close_for_cluster(&self, cluster_id: &str) {
        let completions = {
            let mut state = self.state.lock().await;
            let mut completions: Vec<_> = state
                .operations
                .values()
                .filter(|operation| operation.cluster_id.as_deref() == Some(cluster_id))
                .map(|operation| {
                    operation.cancelled.store(true, Ordering::Release);
                    operation.completion.clone()
                })
                .collect();
            let ids: Vec<_> = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.session.cluster_id == cluster_id)
                .map(|(id, _)| id.clone())
                .collect();
            for id in ids {
                let entry = state.entries.remove(&id).expect("entry exists");
                let expired = Instant::now().duration_since(entry.renewed_at) >= self.lease;
                completions.push(Self::start_cleanup(
                    &self.state,
                    &mut state,
                    id,
                    entry.session,
                    expired,
                ));
            }
            completions
        };
        for completion in completions {
            Self::wait(completion).await;
        }
    }

    pub async fn close_all(&self) {
        let completions = {
            let mut state = self.state.lock().await;
            state.shutting_down = true;
            for (id, entry) in std::mem::take(&mut state.entries) {
                let expired = Instant::now().duration_since(entry.renewed_at) >= self.lease;
                Self::start_cleanup(&self.state, &mut state, id, entry.session, expired);
            }
            state
                .operations
                .values()
                .map(|operation| operation.completion.clone())
                .collect::<Vec<_>>()
        };
        for completion in completions {
            Self::wait(completion).await;
        }
    }

    pub async fn reap_expired(&self) {
        Self::reap(&self.state, self.lease).await;
    }

    async fn reap(shared: &Arc<Mutex<Sessions<F::Client>>>, lease: Duration) {
        let completions = {
            let mut state = shared.lock().await;
            let mut completions: Vec<_> = state
                .operations
                .values()
                .filter(|operation| operation.expired)
                .map(|operation| operation.completion.clone())
                .collect();
            let now = Instant::now();
            let ids: Vec<_> = state
                .entries
                .iter()
                .filter(|(_, entry)| now.duration_since(entry.renewed_at) >= lease)
                .map(|(id, _)| id.clone())
                .collect();
            for id in ids {
                let entry = state.entries.remove(&id).expect("entry exists");
                completions.push(Self::start_cleanup(
                    shared,
                    &mut state,
                    id,
                    entry.session,
                    true,
                ));
            }
            completions
        };
        for completion in completions {
            Self::wait(completion).await;
        }
    }

    fn pending_close(state: &Sessions<F::Client>, id: &str) -> Option<watch::Receiver<bool>> {
        state
            .operations
            .values()
            .find(|operation| operation.closing_session_id.as_deref() == Some(id))
            .map(|operation| operation.completion.clone())
    }

    fn start_cleanup(
        shared: &Arc<Mutex<Sessions<F::Client>>>,
        state: &mut Sessions<F::Client>,
        id: String,
        entry: Arc<SessionEntry<F::Client>>,
        expired: bool,
    ) -> watch::Receiver<bool> {
        entry.closed.store(true, Ordering::Release);
        let (operation_id, done, completion, _) =
            state.register_operation(Some(entry.cluster_id.clone()), Some(id), expired);
        let shared = shared.clone();
        // The operation stays tracked while waiting for a collector's client lock and
        // while native destruction runs. Request cancellation cannot abort this task.
        tokio::spawn(async move {
            entry.close().await;
            shared.lock().await.operations.remove(&operation_id);
            done.send_replace(true);
        });
        completion
    }

    async fn wait(mut completion: watch::Receiver<bool>) {
        let _ = completion.wait_for(|done| *done).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    async fn poll_once<F: Future>(mut future: std::pin::Pin<&mut F>) -> Option<F::Output> {
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match future.as_mut().poll(cx) {
                std::task::Poll::Ready(output) => Some(output),
                std::task::Poll::Pending => None,
            })
        })
        .await
    }

    #[derive(Clone)]
    struct FakeSessionClientFactory(Arc<AtomicUsize>, Arc<tokio::sync::Notify>);

    struct FakeClient(Arc<AtomicUsize>, Arc<tokio::sync::Notify>);

    impl Drop for FakeClient {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            self.1.notify_one();
        }
    }

    impl SessionClientFactory for FakeSessionClientFactory {
        type Client = FakeClient;
        async fn create(&self, _: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
            Ok(FakeClient(self.0.clone(), self.1.clone()))
        }
    }

    fn manager() -> (SessionManager<FakeSessionClientFactory>, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        (
            SessionManager::new(
                Duration::from_secs(15),
                FakeSessionClientFactory(drops.clone(), Arc::new(tokio::sync::Notify::new())),
            ),
            drops,
        )
    }

    // Without reaping at the lease boundary, native clients survive abandoned pages.
    #[tokio::test(start_paused = true)]
    async fn expires_session_after_fifteen_seconds_without_renewal() {
        let (manager, drops) = manager();
        let id = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let held = manager.get(&id).await.unwrap();
        tokio::time::advance(Duration::from_secs(16)).await;
        manager.reap_expired().await;
        assert!(matches!(manager.get(&id).await, Err(SessionError::Expired)));
        assert!(held.clients.lock().await.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // DELETE must report an expired lease even before the five-second reaper runs.
    #[tokio::test(start_paused = true)]
    async fn deleting_expired_unreaped_session_reports_expired_after_release() {
        let (manager, drops) = manager();
        let id = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        manager
            .state
            .lock()
            .await
            .entries
            .get_mut(&id)
            .unwrap()
            .renewed_at = Instant::now() - LEASE_DURATION;
        assert!(matches!(
            manager.close(&id).await,
            Err(SessionError::Expired)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // Cluster/global cleanup must not erase lease expiry while native release is pending.
    #[tokio::test]
    async fn deleting_expired_session_during_cluster_or_global_cleanup_reports_expired() {
        for global in [false, true] {
            let (manager, drops) = manager();
            // Consume the reaper's initial tick before making this lease stale.
            tokio::task::yield_now().await;
            let id = manager
                .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
                .await
                .unwrap();
            let held = manager.get(&id).await.unwrap();
            let guard = held.clients.lock().await;
            manager
                .state
                .lock()
                .await
                .entries
                .get_mut(&id)
                .unwrap()
                .renewed_at = Instant::now() - LEASE_DURATION;
            let closing = tokio::spawn({
                let manager = manager.clone();
                async move {
                    if global {
                        manager.close_all().await;
                    } else {
                        manager.close_for_cluster("cluster-a").await;
                    }
                }
            });
            while !held.closed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            let mut deleting = Box::pin(manager.close(&id));
            assert!(poll_once(deleting.as_mut()).await.is_none());
            drop(guard);
            let outcome = deleting.await;
            closing.await.unwrap();
            assert!(
                matches!(outcome, Err(SessionError::Expired)),
                "global={global}: {outcome:?}"
            );
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    // Renewal must extend only a live lease; stale sessions cannot resurrect.
    #[tokio::test(start_paused = true)]
    async fn renews_live_lease_but_rejects_expired_lease_without_reaper() {
        let (manager, drops) = manager();
        let id = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        manager.get_and_renew(&id).await.unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(manager.get(&id).await.is_ok());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(matches!(
            manager.get_and_renew(&id).await,
            Err(SessionError::Expired)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // Removing entries must release resources once, including externally held handles.
    #[tokio::test]
    async fn close_cluster_and_shutdown_release_each_client_once() {
        let (manager, drops) = manager();
        let first = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let second = manager
            .create("cluster-b", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let third = manager
            .create("cluster-b", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let held = manager.get(&first).await.unwrap();
        assert_eq!(held.cluster_id, "cluster-a");
        manager.close(&first).await.unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            manager.close(&first).await,
            Err(SessionError::Expired)
        ));
        manager.close_for_cluster("cluster-b").await;
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert!(matches!(
            manager.get(&second).await,
            Err(SessionError::Expired)
        ));
        assert!(matches!(
            manager.get(&third).await,
            Err(SessionError::Expired)
        ));
        manager
            .create("cluster-c", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        manager.close_all().await;
        manager.close_all().await;
        assert_eq!(drops.load(Ordering::SeqCst), 4);
        assert!(matches!(
            manager
                .create("cluster-c", DecryptedClusterConfig::plaintext("kafka:9092"))
                .await,
            Err(SessionError::ShuttingDown)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    }

    // Omitting the background task leaves unused sessions alive indefinitely.
    #[tokio::test(start_paused = true)]
    async fn background_reaper_releases_abandoned_sessions() {
        let (manager, drops) = manager();
        manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(20)).await;
        manager.factory.1.notified().await;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // Per-session locks must reject overlap while allowing another cluster to collect.
    #[tokio::test]
    async fn prevents_overlapping_collection_only_for_same_session() {
        let (manager, _) = manager();
        let a = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let b = manager
            .create("cluster-b", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let first = manager.get_and_renew(&a).await.unwrap();
        let other = manager.get_and_renew(&b).await.unwrap();
        let guard = first.try_collect().unwrap();
        assert!(matches!(
            first.try_collect(),
            Err(SessionError::CollectionInProgress)
        ));
        assert!(other.try_collect().is_ok());
        drop(guard);
        assert!(first.try_collect().is_ok());
        manager.close(&a).await.unwrap();
        assert!(matches!(first.try_collect(), Err(SessionError::Expired)));
    }

    #[derive(Clone)]
    struct BlockingTestFactory {
        started: Arc<tokio::sync::Notify>,
        dropping: Arc<tokio::sync::Notify>,
        dropped: Arc<tokio::sync::Notify>,
        release_work: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
        release_drop: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
    }

    impl SessionClientFactory for BlockingTestFactory {
        type Client = ();
        async fn create(&self, _: DecryptedClusterConfig) -> Result<(), SessionError> {
            Ok(())
        }
        async fn test_connection(
            &self,
            _: DecryptedClusterConfig,
        ) -> Result<ConnectionTestResult, SessionError> {
            let factory = self.clone();
            let work = self.release_work.lock().unwrap().take().unwrap();
            let release_drop = self.release_drop.lock().unwrap().take().unwrap();
            tokio::task::spawn_blocking(move || {
                struct ClientDrop(
                    Arc<tokio::sync::Notify>,
                    Arc<tokio::sync::Notify>,
                    std::sync::mpsc::Receiver<()>,
                );
                impl Drop for ClientDrop {
                    fn drop(&mut self) {
                        self.0.notify_one();
                        self.2.recv().unwrap();
                        self.1.notify_one();
                    }
                }
                let client = ClientDrop(factory.dropping, factory.dropped, release_drop);
                factory.started.notify_one();
                work.recv().unwrap();
                drop(client);
                Ok(ConnectionTestResult {
                    broker_count: 1,
                    topic_count: 2,
                    elapsed_ms: 3,
                })
            })
            .await
            .unwrap()
        }
    }

    // Both HTTP cancellation and slow native destruction must stay visible to close operations.
    #[tokio::test]
    async fn connection_tests_remain_owned_until_native_work_and_destructor_finish() {
        for global in [false, true] {
            for cancel in [false, true] {
                let (work_tx, work_rx) = std::sync::mpsc::channel();
                let (drop_tx, drop_rx) = std::sync::mpsc::channel();
                let factory = BlockingTestFactory {
                    started: Arc::new(tokio::sync::Notify::new()),
                    dropping: Arc::new(tokio::sync::Notify::new()),
                    dropped: Arc::new(tokio::sync::Notify::new()),
                    release_work: Arc::new(std::sync::Mutex::new(Some(work_rx))),
                    release_drop: Arc::new(std::sync::Mutex::new(Some(drop_rx))),
                };
                let manager = SessionManager::new(LEASE_DURATION, factory.clone());
                let request = tokio::spawn({
                    let manager = manager.clone();
                    async move {
                        manager
                            .test_connection(
                                (!global).then(|| "cluster-a".into()),
                                DecryptedClusterConfig::plaintext("kafka:9092"),
                            )
                            .await
                    }
                });
                factory.started.notified().await;
                if cancel {
                    request.abort();
                }
                let mut closing = Box::pin(async {
                    if global {
                        manager.close_all().await;
                    } else {
                        manager.close_for_cluster("cluster-a").await;
                    }
                });
                let returned_before_work = poll_once(closing.as_mut()).await.is_some();
                work_tx.send(()).unwrap();
                factory.dropping.notified().await;
                let returned_before_drop =
                    !returned_before_work && poll_once(closing.as_mut()).await.is_some();
                drop_tx.send(()).unwrap();
                factory.dropped.notified().await;
                if !returned_before_work && !returned_before_drop {
                    closing.await;
                }
                let _ = request.await;
                assert!(
                    !returned_before_work,
                    "close lost temporary connection test: global={global} cancel={cancel}"
                );
                assert!(
                    !returned_before_drop,
                    "close returned during native destruction"
                );
            }
        }
    }

    #[derive(Clone)]
    struct SlowFactory {
        started: Arc<tokio::sync::Notify>,
        proceed: Arc<tokio::sync::Notify>,
        drops: Arc<AtomicUsize>,
    }

    impl SessionClientFactory for SlowFactory {
        type Client = FakeClient;
        async fn create(&self, _: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
            self.started.notify_one();
            self.proceed.notified().await;
            Ok(FakeClient(
                self.drops.clone(),
                Arc::new(tokio::sync::Notify::new()),
            ))
        }
    }

    // Shutdown must wait for already-started native creation, otherwise clients outlive close_all.
    #[tokio::test]
    async fn shutdown_waits_for_inflight_client_creation_and_release() {
        let factory = SlowFactory {
            started: Arc::new(tokio::sync::Notify::new()),
            proceed: Arc::new(tokio::sync::Notify::new()),
            drops: Arc::new(AtomicUsize::new(0)),
        };
        let manager = SessionManager::new(Duration::from_secs(15), factory.clone());
        let creating = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
                    .await
            }
        });
        factory.started.notified().await;
        let shutdown = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager.close_all().await;
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must wait for an in-flight factory"
        );
        factory.proceed.notify_one();
        assert!(matches!(
            creating.await.unwrap(),
            Err(SessionError::ShuttingDown)
        ));
        shutdown.await.unwrap();
        assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
    }

    #[derive(Clone)]
    struct BlockingFactory {
        started: Arc<tokio::sync::Notify>,
        release: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
        drops: Arc<AtomicUsize>,
        dropped: Arc<tokio::sync::Notify>,
    }

    impl SessionClientFactory for BlockingFactory {
        type Client = FakeClient;
        async fn create(&self, _: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
            let release = self.release.lock().unwrap().take().unwrap();
            let factory = self.clone();
            tokio::task::spawn_blocking(move || {
                let client = FakeClient(factory.drops.clone(), factory.dropped.clone());
                factory.started.notify_one();
                release.recv().unwrap();
                Ok(client)
            })
            .await
            .unwrap()
        }
    }

    // Aborting a request must not detach its still-running native creation from shutdown.
    #[tokio::test]
    async fn cancelled_creation_remains_tracked_until_native_creation_and_drop_finish() {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let factory = BlockingFactory {
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(std::sync::Mutex::new(Some(release_rx))),
            drops: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(tokio::sync::Notify::new()),
        };
        let manager = SessionManager::new(LEASE_DURATION, factory.clone());
        let creating = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
                    .await
            }
        });
        factory.started.notified().await;
        creating.abort();
        assert!(creating.await.unwrap_err().is_cancelled());
        let mut shutdown = Box::pin(manager.close_all());
        let returned_early = poll_once(shutdown.as_mut()).await.is_some();
        release_tx.send(()).unwrap();
        factory.dropped.notified().await;
        if !returned_early {
            shutdown.await;
        }
        assert!(
            !returned_early,
            "shutdown lost a cancelled creation's blocking task"
        );
        assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
    }

    // Cluster deletion must also drain creation whose API request released its cluster gate.
    #[tokio::test]
    async fn cluster_close_drains_pending_creation_with_live_or_cancelled_caller() {
        for cancel_caller in [false, true] {
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let factory = BlockingFactory {
                started: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(std::sync::Mutex::new(Some(release_rx))),
                drops: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(tokio::sync::Notify::new()),
            };
            let manager = SessionManager::new(LEASE_DURATION, factory.clone());
            let creating = tokio::spawn({
                let manager = manager.clone();
                async move {
                    manager
                        .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
                        .await
                }
            });
            factory.started.notified().await;
            if cancel_caller {
                creating.abort();
            }
            let mut closing = Box::pin(manager.close_for_cluster("cluster-a"));
            let returned_early = poll_once(closing.as_mut()).await.is_some();
            release_tx.send(()).unwrap();
            if !returned_early {
                closing.await;
            }
            assert!(!returned_early, "cluster close lost an in-flight creation");
            if cancel_caller {
                assert!(creating.await.unwrap_err().is_cancelled());
            } else {
                assert!(matches!(
                    creating.await.unwrap(),
                    Err(SessionError::Expired)
                ));
            }
            assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
            assert!(manager.state.lock().await.entries.is_empty());
        }
    }

    async fn assert_cancelled_close_remains_tracked(mode: u8) {
        let (manager, drops) = manager();
        let id = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let held = manager.get(&id).await.unwrap();
        let client_guard = held.clients.lock().await;
        let closing = tokio::spawn({
            let manager = manager.clone();
            async move {
                match mode {
                    0 => {
                        manager.close(&id).await.unwrap();
                    }
                    1 => manager.close_for_cluster("cluster-a").await,
                    _ => manager.close_all().await,
                }
            }
        });
        while !held.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        let mut shutdown = Box::pin(manager.close_all());
        let returned_early = poll_once(shutdown.as_mut()).await.is_some();
        drop(client_guard);
        if !returned_early {
            shutdown.await;
        }
        assert!(
            !returned_early,
            "cancelled close lost a client still held by an old Arc"
        );
        assert!(held.clients.lock().await.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_explicit_close_remains_tracked() {
        assert_cancelled_close_remains_tracked(0).await;
    }

    #[tokio::test]
    async fn cancelled_cluster_close_remains_tracked() {
        assert_cancelled_close_remains_tracked(1).await;
    }

    #[tokio::test]
    async fn cancelled_shutdown_remains_tracked() {
        assert_cancelled_close_remains_tracked(2).await;
    }

    // Closing one session must not consume another session's lease while waiting on native work.
    #[tokio::test]
    async fn slow_close_does_not_block_an_unrelated_sessions_renewal() {
        let (manager, _) = manager();
        let first = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let second = manager
            .create("cluster-b", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        let held = manager.get(&first).await.unwrap();
        let guard = held.clients.lock().await;
        let closing = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager.close(&first).await.unwrap();
            }
        });
        while !held.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let mut renewing = Box::pin(manager.get_and_renew(&second));
        let renewal = poll_once(renewing.as_mut()).await;
        drop(guard);
        closing.await.unwrap();
        assert!(
            renewal.is_some(),
            "unrelated renewal waited on another session's cleanup"
        );
        assert!(renewal.unwrap().is_ok());
    }
}
