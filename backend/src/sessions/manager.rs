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
    admin::AdminClient,
    client::ClientContext,
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, ConsumerContext},
    error::KafkaError,
};
use tokio::{
    sync::{Mutex, MutexGuard},
    time::Instant,
};

use crate::{
    domain::cluster::DecryptedClusterConfig,
    kafka::{client::ConnectionError, config::build_client_properties},
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
    pub admin: AdminClient<SessionContext>,
    // Populated lazily by snapshot collection, never subscribed or auto-committing.
    pub group_consumers: HashMap<String, BaseConsumer<SessionContext>>,
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
                admin,
                group_consumers: HashMap::new(),
                config,
            })
        })
        .await
        .map_err(|_| ConnectionError::Timeout)?
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
    shutting_down: bool,
}

pub struct SessionManager<F: SessionClientFactory = KafkaSessionClientFactory> {
    factory: F,
    lease: Duration,
    state: Arc<Mutex<Sessions<F::Client>>>,
    cluster_gate: Arc<Mutex<()>>,
    cleanup_gate: Arc<Mutex<()>>,
    creation_gate: Arc<Mutex<()>>,
}

impl<F: SessionClientFactory> Clone for SessionManager<F> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            lease: self.lease,
            state: self.state.clone(),
            cluster_gate: self.cluster_gate.clone(),
            cleanup_gate: self.cleanup_gate.clone(),
            creation_gate: self.creation_gate.clone(),
        }
    }
}

impl<F: SessionClientFactory> SessionManager<F> {
    pub fn new(lease: Duration, factory: F) -> Self {
        let state = Arc::new(Mutex::new(Sessions {
            entries: HashMap::<String, LeasedEntry<F::Client>>::new(),
            shutting_down: false,
        }));
        let weak = Arc::downgrade(&state);
        let cleanup_gate = Arc::new(Mutex::new(()));
        let reaper_gate = cleanup_gate.clone();
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
                Self::reap(&state, lease, &reaper_gate).await;
            }
        });
        Self {
            factory,
            lease,
            state,
            cluster_gate: Arc::new(Mutex::new(())),
            cleanup_gate,
            creation_gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn lease_seconds(&self) -> u64 {
        self.lease.as_secs()
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
        let _creation = self.creation_gate.lock().await;
        if self.state.lock().await.shutting_down {
            return Err(SessionError::ShuttingDown);
        }
        let client = self.factory.create(config).await?;
        let session = Arc::new(SessionEntry {
            cluster_id: cluster_id.into(),
            clients: Mutex::new(Some(client)),
            collection_lock: Mutex::new(()),
            closed: AtomicBool::new(false),
        });
        let mut state = self.state.lock().await;
        if state.shutting_down {
            drop(state);
            session.close().await;
            return Err(SessionError::ShuttingDown);
        }
        let id = uuid::Uuid::new_v4().to_string();
        state.entries.insert(
            id.clone(),
            LeasedEntry {
                session,
                renewed_at: Instant::now(),
            },
        );
        Ok(id)
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
        let _cleanup = self.cleanup_gate.lock().await;
        let mut state = self.state.lock().await;
        let entry = state.entries.get_mut(id).ok_or(SessionError::Expired)?;
        let now = Instant::now();
        if now.duration_since(entry.renewed_at) >= self.lease {
            let expired = state.entries.remove(id).expect("entry exists").session;
            drop(state);
            expired.close().await;
            return Err(SessionError::Expired);
        }
        if renew {
            entry.renewed_at = now;
        }
        Ok(entry.session.clone())
    }

    pub async fn close(&self, id: &str) -> Result<(), SessionError> {
        let _cleanup = self.cleanup_gate.lock().await;
        let entry = self
            .state
            .lock()
            .await
            .entries
            .remove(id)
            .ok_or(SessionError::Expired)?;
        entry.session.close().await;
        Ok(())
    }

    pub async fn close_for_cluster(&self, cluster_id: &str) {
        let _cleanup = self.cleanup_gate.lock().await;
        let entries = {
            let mut state = self.state.lock().await;
            let ids: Vec<_> = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.session.cluster_id == cluster_id)
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| state.entries.remove(&id))
                .collect::<Vec<_>>()
        };
        for entry in entries {
            entry.session.close().await;
        }
    }

    pub async fn close_all(&self) {
        self.state.lock().await.shutting_down = true;
        let _creation = self.creation_gate.lock().await;
        let _cleanup = self.cleanup_gate.lock().await;
        let entries = {
            let mut state = self.state.lock().await;
            state.shutting_down = true;
            std::mem::take(&mut state.entries)
        };
        for entry in entries.into_values() {
            entry.session.close().await;
        }
    }

    pub async fn reap_expired(&self) {
        Self::reap(&self.state, self.lease, &self.cleanup_gate).await;
    }

    async fn reap(state: &Mutex<Sessions<F::Client>>, lease: Duration, cleanup_gate: &Mutex<()>) {
        let _cleanup = cleanup_gate.lock().await;
        let expired = {
            let mut state = state.lock().await;
            let now = Instant::now();
            let ids: Vec<_> = state
                .entries
                .iter()
                .filter(|(_, entry)| now.duration_since(entry.renewed_at) >= lease)
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| state.entries.remove(&id))
                .collect::<Vec<_>>()
        };
        for entry in expired {
            entry.session.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

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
}
