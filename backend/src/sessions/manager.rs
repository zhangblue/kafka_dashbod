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
    domain::{
        cluster::DecryptedClusterConfig,
        topic_admin::{
            CreateTopicInput, TopicAdminError, TopicConfigUpdateOutcome, TopicConfigValue,
            UpdateTopicConfigsInput,
        },
    },
    kafka::{
        client::{ConnectionError, ConnectionTestResult},
        config::build_client_properties,
        group_consumers::GroupConsumerCache,
        topic_admin::TopicAdminClient,
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicOperationError {
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    Admin(#[from] TopicAdminError),
}

pub trait SessionClientFactory: Clone + Send + Sync + 'static {
    type Client: Send + 'static;
    // 需要新会话或告警客户端时按配置创建资源，由工厂实现负责原生创建完成。
    fn create(
        &self,
        config: DecryptedClusterConfig,
    ) -> impl Future<Output = Result<Self::Client, SessionError>> + Send;

    // 连接测试调用时运行临时探测并等待资源释放，不建立监控会话。
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

    // 页面或告警需要样本时由实现提供快照；默认实现拒绝未支持采集的工厂。
    fn snapshot(
        &self,
        _client: &mut Self::Client,
        _cluster_id: &str,
    ) -> impl Future<Output = Result<crate::domain::snapshot::ClusterSnapshot, SessionError>> + Send
    {
        async { Err(SessionError::Connection(ConnectionError::InvalidConfig)) }
    }

    // 会话接受创建请求时由实现执行；默认拒绝，避免未实现工厂意外具有管理能力。
    fn create_topic(
        &self,
        _client: &mut Self::Client,
        _input: &CreateTopicInput,
    ) -> impl Future<Output = Result<(), TopicAdminError>> + Send {
        async { Err(TopicAdminError::Unavailable) }
    }

    // 会话接受删除请求时由实现执行；默认返回不可用，不提供隐式管理操作。
    fn delete_topic(
        &self,
        _client: &mut Self::Client,
        _topic: &str,
    ) -> impl Future<Output = Result<(), TopicAdminError>> + Send {
        async { Err(TopicAdminError::Unavailable) }
    }

    // 打开配置界面时由实现读取实时参数；默认返回不可用，不以示例值代替。
    fn describe_topic_configs(
        &self,
        _client: &mut Self::Client,
        _topic: &str,
    ) -> impl Future<Output = Result<Vec<TopicConfigValue>, TopicAdminError>> + Send {
        async { Err(TopicAdminError::Unavailable) }
    }

    // 保存配置时由实现增量修改并确认；默认拒绝未实现的配置管理能力。
    fn update_topic_configs(
        &self,
        _client: &mut Self::Client,
        _topic: &str,
        _input: &UpdateTopicConfigsInput,
    ) -> impl Future<Output = Result<TopicConfigUpdateOutcome, TopicAdminError>> + Send {
        async { Err(TopicAdminError::Unavailable) }
    }
}

/// Suppress native messages, which may contain decrypted credentials.
pub struct SessionContext;
impl ClientContext for SessionContext {
    // 原生日志回调触发时丢弃文本，防止连接配置进入日志。
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    // 原生错误回调触发时丢弃原始诊断，避免泄漏敏感连接信息。
    fn error(&self, _: KafkaError, _: &str) {}
    // 原生统计回调触发时丢弃原始载荷，避免输出连接相关信息。
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

    // 创建真实会话或告警客户端时在阻塞线程构建管理客户端，等待完整创建结果。
    async fn create(&self, config: DecryptedClusterConfig) -> Result<Self::Client, SessionError> {
        // 原生创建可能启动轮询线程，将其放到阻塞池并由拥有者等待，避免阻塞异步执行器。
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

    // 页面或告警需要样本时通过工厂采集快照，返回前等待全部原生采集工作。
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
        // 采集超时不取消 C 调用；先收齐任务，再允许释放客户端和共享操作锁。
        probe.finish().await;
        Ok(snapshot)
    }

    // 创建 Topic 时通过会话管理适配执行已校验请求。
    async fn create_topic(
        &self,
        client: &mut Self::Client,
        input: &CreateTopicInput,
    ) -> Result<(), TopicAdminError> {
        TopicAdminClient::new(client.admin.clone(), client.config.connection_timeout)
            .create(input)
            .await
    }

    // 删除 Topic 时通过会话管理适配执行受保护的目标删除。
    async fn delete_topic(
        &self,
        client: &mut Self::Client,
        topic: &str,
    ) -> Result<(), TopicAdminError> {
        TopicAdminClient::new(client.admin.clone(), client.config.connection_timeout)
            .delete(topic)
            .await
    }

    // 打开配置界面时通过会话读取当前 Topic 参数，不持久化副本。
    async fn describe_topic_configs(
        &self,
        client: &mut Self::Client,
        topic: &str,
    ) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
        TopicAdminClient::new(client.admin.clone(), client.config.connection_timeout)
            .describe(topic)
            .await
    }

    // 保存 Topic 配置时通过会话增量修改并保留写后确认状态。
    async fn update_topic_configs(
        &self,
        client: &mut Self::Client,
        topic: &str,
        input: &UpdateTopicConfigsInput,
    ) -> Result<TopicConfigUpdateOutcome, TopicAdminError> {
        TopicAdminClient::new(client.admin.clone(), client.config.connection_timeout)
            .update(topic, input)
            .await
    }
}

pub struct SessionEntry<C> {
    pub cluster_id: String,
    /// Held throughout collection and administration; close takes even surviving Arc handles.
    pub clients: Mutex<Option<C>>,
    collection_lock: Mutex<()>,
    closed: AtomicBool,
}

impl<C: Send + 'static> SessionEntry<C> {
    // 快照开始时尝试取得会话互斥锁，拒绝重叠采集并复核关闭状态。
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

    // 管理操作排队时取得与采集共享的互斥锁，获锁后再次拒绝已关闭会话。
    async fn lock_operation(&self) -> Result<MutexGuard<'_, ()>, SessionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SessionError::Expired);
        }
        // 管理操作可以排队；持锁后必须再查关闭标记，防止排队期间关闭后仍执行写入。
        let guard = self.collection_lock.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(SessionError::Expired);
        }
        Ok(guard)
    }

    // 释放会话时先拒绝新操作，再按采集和客户端锁顺序等待在途工作及阻塞析构。
    async fn close(&self) {
        // 先阻止新的采集或管理操作，再沿相同锁顺序排空已接受工作，避免锁反转。
        self.closed.store(true, Ordering::Release);
        // Match the collection/admin lock order and drain queued operations first.
        let _operation = self.collection_lock.lock().await;
        let clients = self.clients.lock().await.take();
        // Native client destruction can block while its poll threads terminate.
        if let Some(clients) = clients {
            // 析构可能等待原生轮询线程；在阻塞池完成并等待后，关闭才能对外报告完成。
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
    session_id: Option<String>,
    closing_session_id: Option<String>,
    expired: bool,
    cancelled: Arc<AtomicBool>,
    completion: watch::Receiver<bool>,
}

impl<C> Sessions<C> {
    // 接受任务时在管理器锁内登记完成记录，供取消、关闭和退出持续追踪。
    fn register_operation(
        &mut self,
        cluster_id: Option<String>,
        session_id: Option<String>,
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
                session_id,
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
    // 共享句柄被复制时保留同一状态与生命周期协调对象。
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
    // 连接测试调用时运行临时探测并等待资源释放，不建立监控会话。
    pub async fn test_connection(
        &self,
        cluster_id: Option<String>,
        config: DecryptedClusterConfig,
    ) -> Result<ConnectionTestResult, SessionError> {
        let mut state = self.state.lock().await;
        if state.shutting_down {
            return Err(SessionError::ShuttingDown);
        }
        // 登记和创建拥有者任务都在状态锁内，关闭不会漏掉已接受但尚未调度的连接测试。
        let (operation_id, done, _, _) = state.register_operation(cluster_id, None, None, false);
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

    // 页面或告警需要样本时通过工厂采集快照，返回前等待全部原生采集工作。
    pub async fn snapshot(
        &self,
        id: &str,
    ) -> Result<crate::domain::snapshot::ClusterSnapshot, SessionError> {
        // 接受快照前先检查并续租；拥有者任务继续持锁直至全部原生采集收尾。
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

    // 创建 Topic 时通过会话管理适配执行已校验请求。
    pub async fn create_topic(
        &self,
        id: &str,
        input: CreateTopicInput,
    ) -> Result<(), TopicOperationError> {
        self.topic_operation(id, move |factory, client| {
            Box::pin(async move { factory.create_topic(client, &input).await })
        })
        .await
    }

    // 删除 Topic 时通过会话管理适配执行受保护的目标删除。
    pub async fn delete_topic(&self, id: &str, topic: &str) -> Result<(), TopicOperationError> {
        let topic = topic.to_owned();
        self.topic_operation(id, move |factory, client| {
            Box::pin(async move { factory.delete_topic(client, &topic).await })
        })
        .await
    }

    // 打开配置界面时通过会话读取当前 Topic 参数，不持久化副本。
    pub async fn describe_topic_configs(
        &self,
        id: &str,
        topic: &str,
    ) -> Result<Vec<TopicConfigValue>, TopicOperationError> {
        let topic = topic.to_owned();
        self.topic_operation(id, move |factory, client| {
            Box::pin(async move { factory.describe_topic_configs(client, &topic).await })
        })
        .await
    }

    // 保存 Topic 配置时通过会话增量修改并保留写后确认状态。
    pub async fn update_topic_configs(
        &self,
        id: &str,
        topic: &str,
        input: UpdateTopicConfigsInput,
    ) -> Result<TopicConfigUpdateOutcome, TopicOperationError> {
        let topic = topic.to_owned();
        self.topic_operation(id, move |factory, client| {
            Box::pin(async move { factory.update_topic_configs(client, &topic, &input).await })
        })
        .await
    }

    // 接受管理操作时续租并登记拥有者任务，与快照互斥且取消 HTTP 后仍追踪至任务析构。
    async fn topic_operation<T: Send + 'static>(
        &self,
        id: &str,
        operation: impl for<'a> FnOnce(
            &'a F,
            &'a mut F::Client,
        )
            -> futures::future::BoxFuture<'a, Result<T, TopicAdminError>>
        + Send
        + 'static,
    ) -> Result<T, TopicOperationError> {
        let session = self.get_and_renew(id).await?;
        let mut state = self.state.lock().await;
        // Close may have removed the session while registration waited for the state lock.
        if state
            .entries
            .get(id)
            .is_none_or(|entry| !Arc::ptr_eq(&entry.session, &session))
        {
            return Err(SessionError::Expired.into());
        }
        let (operation_id, done, _, _) = state.register_operation(
            Some(session.cluster_id.clone()),
            Some(id.to_owned()),
            None,
            false,
        );
        let factory = self.factory.clone();
        // 锁内先登记再派发，使关闭能看见尚未运行的任务；管理和快照共用会话操作锁。
        // Registration and spawn are atomic to close, even before this task is scheduled.
        let task = tokio::spawn(async move {
            let _operation = session.lock_operation().await?;
            let mut clients = session.clients.lock().await;
            let client = clients.as_mut().ok_or(SessionError::Expired)?;
            operation(&factory, client)
                .await
                .map_err(TopicOperationError::Admin)
        });
        let shared = self.state.clone();
        let (reply, result) = oneshot::channel();
        // 独立等待者连同任务捕获值的析构一起等待，再删除完成记录；取消 HTTP 仅丢弃接收端。
        // Joining includes task capture destruction; HTTP cancellation only drops its receiver.
        tokio::spawn(async move {
            let outcome = task
                .await
                .unwrap_or_else(|_| Err(TopicAdminError::Unavailable.into()));
            shared.lock().await.operations.remove(&operation_id);
            done.send_replace(true);
            let _ = reply.send(outcome);
        });
        drop(state);
        result.await.map_err(|_| TopicAdminError::Unavailable)?
    }

    // 应用初始化时创建会话管理器，并以弱引用启动每五秒一次的过期清理。
    pub fn new(lease: Duration, factory: F) -> Self {
        let state = Arc::new(Mutex::new(Sessions {
            entries: HashMap::<String, LeasedEntry<F::Client>>::new(),
            operations: HashMap::new(),
            shutting_down: false,
        }));
        // 清理器只持弱引用，管理器消失后不再永久维持状态；每五秒回收遗弃会话。
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

    // 创建会话响应时返回租约秒数，供调用方安排续租。
    pub fn lease_seconds(&self) -> u64 {
        self.lease.as_secs()
    }

    /// Read correlation metadata without renewing leases or taking ownership of clients.
    // 关联请求与集群时只读取元数据，不续租或占有客户端。
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
    // 集群读取、创建会话与删除前取得统一门锁，防止删除后留下孤立会话。
    pub async fn cluster_operation(&self) -> MutexGuard<'_, ()> {
        self.cluster_gate.lock().await
    }

    // 页面需要采样时登记并创建短租约会话；取消调用后仍跟踪创建和必要的释放。
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
            state.register_operation(Some(cluster_id.clone()), None, None, false);
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
                        // 创建已完成但调用方取消或管理器正在关闭，必须先释放资源，不能留下无人续租的连接。
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

    // 内部读取会话时检查有效租约但不续期，过期时等待资源释放。
    pub async fn get(&self, id: &str) -> Result<Arc<SessionEntry<F::Client>>, SessionError> {
        self.lookup(id, false).await
    }

    // 快照或管理操作开始时读取有效会话并续租，过期会话不得复活。
    pub async fn get_and_renew(
        &self,
        id: &str,
    ) -> Result<Arc<SessionEntry<F::Client>>, SessionError> {
        self.lookup(id, true).await
    }

    // 查询会话时检查租约，必要时启动并等待释放，再决定是否续租。
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

    // 调用方关闭单个会话时移除可见条目，复用已有清理并等待实际释放。
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

    // 删除或切换集群时取消在途创建并等待该集群所有已接受任务及资源释放。
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
        // 状态锁已经释放，慢速析构只影响本次关闭，不阻塞无关会话的读取或续租。
        for completion in completions {
            Self::wait(completion).await;
        }
    }

    // 进程退出时先禁止接受新任务，再等待所有会话和在途操作释放。
    pub async fn close_all(&self) {
        let completions = {
            let mut state = self.state.lock().await;
            // 先封闭接收入口，再收集包括在途创建在内的完成信号，确保退出不遗漏原生任务。
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

    // 主动清理调用时按当前租约回收已过期会话并等待释放。
    pub async fn reap_expired(&self) {
        Self::reap(&self.state, self.lease).await;
    }

    // 定时或主动清理时先移除过期条目，再在全局锁外等待各项释放完成。
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

    // 查询已移除会话时查找仍在进行的关闭记录，以等待实际资源释放。
    fn pending_close(state: &Sessions<F::Client>, id: &str) -> Option<watch::Receiver<bool>> {
        state
            .operations
            .values()
            .find(|operation| operation.closing_session_id.as_deref() == Some(id))
            .map(|operation| operation.completion.clone())
    }

    // 关闭或租约过期时先标记关闭并登记清理任务，持续追踪至原生析构及已接受任务结束。
    fn start_cleanup(
        shared: &Arc<Mutex<Sessions<F::Client>>>,
        state: &mut Sessions<F::Client>,
        id: String,
        entry: Arc<SessionEntry<F::Client>>,
        expired: bool,
    ) -> watch::Receiver<bool> {
        // 先标记关闭，已排队的管理任务随后获锁也必须退出；清理任务则持续保有资源。
        entry.closed.store(true, Ordering::Release);
        let operations: Vec<_> = state
            .operations
            .values()
            .filter(|operation| operation.session_id.as_deref() == Some(&id))
            .map(|operation| operation.completion.clone())
            .collect();
        let (operation_id, done, completion, _) =
            state.register_operation(Some(entry.cluster_id.clone()), None, Some(id), expired);
        let shared = shared.clone();
        // The operation stays tracked while waiting for a collector's client lock and
        // while native destruction runs. Request cancellation cannot abort this task.
        tokio::spawn(async move {
            // 先等待会话锁和原生析构，再等待任务捕获值析构，最后发布完成信号。
            entry.close().await;
            for operation in operations {
                Self::wait(operation).await;
            }
            shared.lock().await.operations.remove(&operation_id);
            done.send_replace(true);
        });
        completion
    }

    // 关闭路径等待共享完成信号；调用方取消等待不会取消资源清理任务。
    async fn wait(mut completion: watch::Receiver<bool>) {
        let _ = completion.wait_for(|done| *done).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        snapshot::ClusterSnapshot,
        topic_admin::{
            CreateTopicInput, TopicAdminError, TopicConfigSource, TopicConfigUpdateOutcome,
            TopicConfigValue, UpdateTopicConfigsInput,
        },
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    // 生命周期测试推进一次异步状态，用于观察尚未完成的操作而不一直等待。
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
        // 测试对象析构时记录或控制释放阶段，验证资源最终释放及等待顺序。
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            self.1.notify_one();
        }
    }

    impl SessionClientFactory for FakeSessionClientFactory {
        type Client = FakeClient;
        // 会话测试请求创建时提供对应的可控客户端，模拟创建或资源释放生命周期。
        async fn create(&self, _: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
            Ok(FakeClient(self.0.clone(), self.1.clone()))
        }
    }

    // 会话测试准备时创建十五秒租约管理器，并返回共享析构计数。
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
    // 验证十五秒未续租的会话被回收并释放客户端。
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
    // 验证删除尚未清理的过期会话时先释放再报告过期。
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
    // 验证集群或全局清理中的过期会话仍正确报告过期。
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
    // 验证有效租约可续期，未经过清理器的过期租约仍不可复活。
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
    // 验证集群关闭及退出只释放每个客户端一次。
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
    // 验证后台清理器回收被页面遗弃的会话。
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
    // 验证同会话采集互斥而不同会话可以独立进行。
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
        // 会话测试请求创建时提供对应的可控客户端，模拟创建或资源释放生命周期。
        async fn create(&self, _: DecryptedClusterConfig) -> Result<(), SessionError> {
            Ok(())
        }
        // 连接生命周期测试调用时阻塞模拟工作及析构，用于证明取消后仍受管理器追踪。
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
                    // 测试对象析构时记录或控制释放阶段，验证资源最终释放及等待顺序。
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
    // 验证取消连接测试后管理器仍等待原生工作和析构完成。
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
        // 会话测试请求创建时提供对应的可控客户端，模拟创建或资源释放生命周期。
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
    // 验证退出等待在途客户端创建及其资源释放。
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
        // 会话测试请求创建时提供对应的可控客户端，模拟创建或资源释放生命周期。
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
    // 验证取消创建请求后原生创建和析构仍受管理器追踪。
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
    // 验证集群关闭等待未完成创建，无论调用方是否取消。
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

    // 取消关闭测试按不同关闭入口验证清理仍被管理器追踪至资源释放。
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
    // 验证显式关闭被调用方取消后清理仍被追踪。
    async fn cancelled_explicit_close_remains_tracked() {
        assert_cancelled_close_remains_tracked(0).await;
    }

    #[tokio::test]
    // 验证集群关闭被调用方取消后清理仍被追踪。
    async fn cancelled_cluster_close_remains_tracked() {
        assert_cancelled_close_remains_tracked(1).await;
    }

    #[tokio::test]
    // 验证退出等待被调用方取消后清理仍被追踪。
    async fn cancelled_shutdown_remains_tracked() {
        assert_cancelled_close_remains_tracked(2).await;
    }

    // Closing one session must not consume another session's lease while waiting on native work.
    #[tokio::test]
    // 验证慢速释放不会阻碍无关会话续租。
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

    #[derive(Clone, Copy, Debug)]
    enum TopicOperation {
        Create,
        Delete,
        Describe,
        Update,
    }

    const TOPIC_OPERATIONS: [TopicOperation; 4] = [
        TopicOperation::Create,
        TopicOperation::Delete,
        TopicOperation::Describe,
        TopicOperation::Update,
    ];

    impl TopicOperation {
        // 管理互斥测试按枚举选择四类操作，核对适配结果以复用同一生命周期断言。
        async fn run(
            self,
            manager: &SessionManager<BlockingTopicFactory>,
            id: &str,
        ) -> Result<(), TopicOperationError> {
            match self {
                Self::Create => {
                    manager
                        .create_topic(
                            id,
                            CreateTopicInput {
                                name: "orders".into(),
                                partitions: 2,
                                replication_factor: Some(1),
                                configs: [("retention.ms".into(), "60000".into())].into(),
                            },
                        )
                        .await
                }
                Self::Delete => manager.delete_topic(id, "orders").await,
                Self::Describe => {
                    manager
                        .describe_topic_configs(id, "orders")
                        .await
                        .map(|values| {
                            assert_eq!(
                                values,
                                vec![TopicConfigValue {
                                    key: "retention.ms".into(),
                                    value: Some("60000".into()),
                                    source: TopicConfigSource::TopicOverride,
                                    read_only: false,
                                }]
                            );
                        })
                }
                Self::Update => manager
                    .update_topic_configs(
                        id,
                        "orders",
                        UpdateTopicConfigsInput {
                            changes: [("retention.ms".into(), Some("60000".into()))].into(),
                            expected_values: [("retention.ms".into(), None)].into(),
                        },
                    )
                    .await
                    .map(|outcome| {
                        assert_eq!(outcome, TopicConfigUpdateOutcome::AppliedButUnconfirmed);
                    }),
            }
        }
    }

    // 管理操作测试构造固定配置值，供读取结果断言使用。
    fn topic_config() -> TopicConfigValue {
        TopicConfigValue {
            key: "retention.ms".into(),
            value: Some("60000".into()),
            source: TopicConfigSource::TopicOverride,
            read_only: false,
        }
    }

    #[derive(Clone, Default)]
    struct BlockingTopicFactory {
        snapshot_started: Arc<tokio::sync::Notify>,
        release_snapshot: Arc<tokio::sync::Notify>,
        block_snapshot: Arc<AtomicBool>,
        started: Arc<tokio::sync::Notify>,
        proceed: Arc<tokio::sync::Notify>,
        finishing: Arc<tokio::sync::Notify>,
        finish: Arc<tokio::sync::Notify>,
        block_finish: bool,
        dropping: Arc<tokio::sync::Notify>,
        release_drop: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
        drops: Arc<AtomicUsize>,
        error: Option<TopicAdminError>,
    }

    struct BlockingTopicClient(BlockingTopicFactory);

    impl Drop for BlockingTopicClient {
        // 测试对象析构时记录或控制释放阶段，验证资源最终释放及等待顺序。
        fn drop(&mut self) {
            self.0.dropping.notify_one();
            if let Some(release) = self.0.release_drop.lock().unwrap().take() {
                let _ = release.recv();
            }
            self.0.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl BlockingTopicFactory {
        // 管理操作替身收到请求时停在可控生命周期阶段，便于验证取消与关闭等待。
        async fn operation(&self) -> Result<(), TopicAdminError> {
            self.started.notify_one();
            self.proceed.notified().await;
            if self.block_finish {
                self.finishing.notify_one();
                self.finish.notified().await;
            }
            self.error.clone().map_or(Ok(()), Err)
        }
    }

    impl SessionClientFactory for BlockingTopicFactory {
        type Client = BlockingTopicClient;

        // 会话测试请求创建时提供对应的可控客户端，模拟创建或资源释放生命周期。
        async fn create(&self, _: DecryptedClusterConfig) -> Result<Self::Client, SessionError> {
            Ok(BlockingTopicClient(self.clone()))
        }

        // 会话测试采集时返回可控快照，并按替身设置停在指定阶段以验证互斥。
        async fn snapshot(
            &self,
            _: &mut Self::Client,
            cluster_id: &str,
        ) -> Result<ClusterSnapshot, SessionError> {
            if self.block_snapshot.swap(false, Ordering::SeqCst) {
                self.snapshot_started.notify_one();
                self.release_snapshot.notified().await;
            }
            Ok(ClusterSnapshot {
                cluster_id: cluster_id.into(),
                collected_at: "2026-10-21T00:00:00Z".into(),
                topics: vec![],
                consumer_group_ids: vec![],
                consumer_groups: vec![],
                partial_failures: vec![],
            })
        }

        // 管理测试调用时核对传入参数，并通过可控替身验证操作生命周期。
        async fn create_topic(
            &self,
            _: &mut Self::Client,
            input: &CreateTopicInput,
        ) -> Result<(), TopicAdminError> {
            assert_eq!(input.name, "orders");
            assert_eq!(input.partitions, 2);
            assert_eq!(input.replication_factor, Some(1));
            assert_eq!(input.configs.get("retention.ms").unwrap(), "60000");
            self.operation().await
        }

        // 管理测试调用时核对传入参数，并通过可控替身验证操作生命周期。
        async fn delete_topic(
            &self,
            _: &mut Self::Client,
            topic: &str,
        ) -> Result<(), TopicAdminError> {
            assert_eq!(topic, "orders");
            self.operation().await
        }

        // 管理测试调用时核对传入参数，并通过可控替身验证操作生命周期。
        async fn describe_topic_configs(
            &self,
            _: &mut Self::Client,
            topic: &str,
        ) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
            assert_eq!(topic, "orders");
            self.operation().await?;
            Ok(vec![topic_config()])
        }

        // 管理测试调用时核对传入参数，并通过可控替身验证操作生命周期。
        async fn update_topic_configs(
            &self,
            _: &mut Self::Client,
            topic: &str,
            input: &UpdateTopicConfigsInput,
        ) -> Result<TopicConfigUpdateOutcome, TopicAdminError> {
            assert_eq!(topic, "orders");
            assert_eq!(input.changes["retention.ms"], Some("60000".into()));
            assert_eq!(input.expected_values["retention.ms"], None);
            self.operation().await?;
            Ok(TopicConfigUpdateOutcome::AppliedButUnconfirmed)
        }
    }

    // 管理生命周期测试准备时创建带阻塞替身的会话并返回其标识。
    async fn topic_manager(
        factory: BlockingTopicFactory,
    ) -> (SessionManager<BlockingTopicFactory>, String) {
        let manager = SessionManager::new(LEASE_DURATION, factory);
        let id = manager
            .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
            .await
            .unwrap();
        (manager, id)
    }

    // 管理生命周期测试等待指定阶段信号，并用超时防止失败时永久挂起。
    async fn wait_for_topic_signal(signal: &tokio::sync::Notify) {
        tokio::time::timeout(Duration::from_secs(5), signal.notified())
            .await
            .expect("session operation did not reach the expected lifecycle phase");
    }

    // A separate admin mutex would let writes overlap collection or make snapshots wait.
    #[tokio::test]
    // 验证管理操作等待采集，且新的重叠快照立即被拒绝。
    async fn topic_operations_wait_for_snapshot_and_reject_overlapping_snapshots() {
        for operation in TOPIC_OPERATIONS {
            let factory = BlockingTopicFactory::default();
            factory.block_snapshot.store(true, Ordering::SeqCst);
            let (manager, id) = topic_manager(factory.clone()).await;
            let snapshot = tokio::spawn({
                let manager = manager.clone();
                let id = id.clone();
                async move { manager.snapshot(&id).await }
            });
            wait_for_topic_signal(&factory.snapshot_started).await;
            let mut request = Box::pin(operation.run(&manager, &id));
            assert!(poll_once(request.as_mut()).await.is_none());
            tokio::task::yield_now().await;
            let mut started = Box::pin(factory.started.notified());
            assert!(poll_once(started.as_mut()).await.is_none());
            factory.release_snapshot.notify_one();
            snapshot.await.unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(5), started)
                .await
                .expect("queued operation never started after snapshot completion");
            assert!(matches!(
                manager.snapshot(&id).await,
                Err(SessionError::CollectionInProgress)
            ));
            let other = manager
                .create("cluster-b", DecryptedClusterConfig::plaintext("kafka:9092"))
                .await
                .unwrap();
            assert_eq!(
                manager.snapshot(&other).await.unwrap().cluster_id,
                "cluster-b"
            );
            factory.proceed.notify_one();
            request.await.unwrap();
            assert!(manager.snapshot(&id).await.is_ok());
            manager.close_all().await;
        }
    }

    // Returning CollectionInProgress for writes would discard an authorized queued operation.
    #[tokio::test]
    // 验证同会话的管理操作按顺序执行。
    async fn topic_operations_wait_for_another_topic_operation() {
        for operation in TOPIC_OPERATIONS {
            let factory = BlockingTopicFactory::default();
            let (manager, id) = topic_manager(factory.clone()).await;
            let mut first = Box::pin(TopicOperation::Create.run(&manager, &id));
            assert!(poll_once(first.as_mut()).await.is_none());
            wait_for_topic_signal(&factory.started).await;
            let mut second = Box::pin(operation.run(&manager, &id));
            assert!(poll_once(second.as_mut()).await.is_none());
            tokio::task::yield_now().await;
            let mut second_started = Box::pin(factory.started.notified());
            assert!(poll_once(second_started.as_mut()).await.is_none());
            factory.proceed.notify_one();
            first.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), second_started)
                .await
                .expect("queued operation never started after administration completed");
            assert!(matches!(
                manager.snapshot(&id).await,
                Err(SessionError::CollectionInProgress)
            ));
            factory.proceed.notify_one();
            second.await.unwrap();
            manager.close_all().await;
        }
    }

    // Dropping HTTP must not drop the admin future or unlock clients before its final await.
    #[tokio::test]
    // 验证取消管理请求后关闭仍等待其工作与析构结束。
    async fn cancelled_topic_operations_remain_owned_until_close_and_destructor_finish() {
        for operation in TOPIC_OPERATIONS {
            for mode in 0..3 {
                let (release_drop, drop_rx) = std::sync::mpsc::channel();
                let factory = BlockingTopicFactory {
                    block_finish: true,
                    release_drop: Arc::new(std::sync::Mutex::new(Some(drop_rx))),
                    ..BlockingTopicFactory::default()
                };
                let (manager, id) = topic_manager(factory.clone()).await;
                let request = tokio::spawn({
                    let manager = manager.clone();
                    let id = id.clone();
                    async move { operation.run(&manager, &id).await }
                });
                wait_for_topic_signal(&factory.started).await;
                request.abort();
                assert!(request.await.unwrap_err().is_cancelled());
                let mut closing = Box::pin(async {
                    match mode {
                        0 => manager.close(&id).await.unwrap(),
                        1 => manager.close_for_cluster("cluster-a").await,
                        _ => manager.close_all().await,
                    }
                });
                assert!(poll_once(closing.as_mut()).await.is_none());
                assert_eq!(factory.drops.load(Ordering::SeqCst), 0);
                factory.proceed.notify_one();
                wait_for_topic_signal(&factory.finishing).await;
                assert!(poll_once(closing.as_mut()).await.is_none());
                factory.finish.notify_one();
                wait_for_topic_signal(&factory.dropping).await;
                assert!(poll_once(closing.as_mut()).await.is_none());
                release_drop.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(5), closing)
                    .await
                    .expect("close deadlocked with an active topic operation");
                assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
            }
        }
    }

    // Failing the second closed check could perform an admin request after close started.
    #[tokio::test]
    // 验证排队管理操作获锁后拒绝已关闭会话。
    async fn queued_topic_operations_reject_a_closed_session() {
        for operation in TOPIC_OPERATIONS {
            let factory = BlockingTopicFactory::default();
            let (manager, id) = topic_manager(factory.clone()).await;
            let held = manager.get(&id).await.unwrap();
            let collection = held.try_collect().unwrap();
            let mut request = Box::pin(operation.run(&manager, &id));
            assert!(poll_once(request.as_mut()).await.is_none());
            tokio::task::yield_now().await;
            let mut closing = Box::pin(manager.close(&id));
            let closed_immediately = poll_once(closing.as_mut()).await;
            assert!(held.closed.load(Ordering::Acquire));
            drop(collection);
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), request)
                    .await
                    .expect("queued operation ran after the session closed"),
                Err(TopicOperationError::Session(SessionError::Expired))
            ));
            if closed_immediately.is_none() {
                closing.await.unwrap();
            }
            let mut started = Box::pin(factory.started.notified());
            assert!(poll_once(started.as_mut()).await.is_none());
        }
    }

    // Administration must renew leases and preserve Kafka errors without locking out snapshots.
    #[tokio::test(start_paused = true)]
    // 验证管理操作续租且保留稳定的管理错误。
    async fn topic_operations_renew_live_leases_and_preserve_admin_errors() {
        for operation in TOPIC_OPERATIONS {
            let factory = BlockingTopicFactory {
                error: Some(TopicAdminError::Authorization),
                ..BlockingTopicFactory::default()
            };
            let (manager, id) = topic_manager(factory.clone()).await;
            manager
                .state
                .lock()
                .await
                .entries
                .get_mut(&id)
                .unwrap()
                .renewed_at = Instant::now() - Duration::from_secs(10);
            let mut request = Box::pin(operation.run(&manager, &id));
            assert!(poll_once(request.as_mut()).await.is_none());
            wait_for_topic_signal(&factory.started).await;
            tokio::time::advance(Duration::from_secs(6)).await;
            assert!(manager.get(&id).await.is_ok());
            factory.proceed.notify_one();
            assert!(matches!(
                request.await,
                Err(TopicOperationError::Admin(TopicAdminError::Authorization))
            ));
            assert!(manager.snapshot(&id).await.is_ok());
            manager.close(&id).await.unwrap();
            assert!(matches!(
                operation.run(&manager, &id).await,
                Err(TopicOperationError::Session(SessionError::Expired))
            ));
        }
    }

    struct UnscheduledTopicFactory {
        shared: Arc<UnscheduledTopicState>,
        block_drop: bool,
    }

    struct UnscheduledTopicState {
        track_next_clone: AtomicBool,
        dropping: std::sync::mpsc::Sender<()>,
        release_drop: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }

    impl Clone for UnscheduledTopicFactory {
        // 调度边界测试复制工厂时标记下一份捕获值，使其析构可控以验证关闭等待。
        fn clone(&self) -> Self {
            Self {
                shared: self.shared.clone(),
                block_drop: self.shared.track_next_clone.swap(false, Ordering::SeqCst),
            }
        }
    }

    impl Drop for UnscheduledTopicFactory {
        // 测试对象析构时记录或控制释放阶段，验证资源最终释放及等待顺序。
        fn drop(&mut self) {
            if self.block_drop {
                let _ = self.shared.dropping.send(());
                if let Some(release) = self.shared.release_drop.lock().unwrap().take() {
                    let _ = release.recv();
                }
            }
        }
    }

    impl SessionClientFactory for UnscheduledTopicFactory {
        type Client = ();

        // 会话测试请求创建时提供对应的可控客户端，模拟创建或资源释放生命周期。
        async fn create(&self, _: DecryptedClusterConfig) -> Result<Self::Client, SessionError> {
            Ok(())
        }

        // 未调度任务测试意外进入适配器时立即失败，确保关闭检查发生在管理调用之前。
        async fn create_topic(
            &self,
            _: &mut Self::Client,
            _: &CreateTopicInput,
        ) -> Result<(), TopicAdminError> {
            panic!("closed session must reject the task before entering the adapter");
        }
    }

    // A lock cannot track a spawned task that has not yet run or joined its wait queue.
    #[test]
    // 验证关闭也等待尚未调度的已接受任务及其捕获工厂析构。
    fn close_waits_for_unscheduled_topic_task_and_its_factory_drop() {
        for mode in 0..3 {
            for cancel_caller in [false, true] {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap();
                let (dropping, drop_started) = std::sync::mpsc::channel();
                let (release_drop, drop_release) = std::sync::mpsc::channel();
                let shared = Arc::new(UnscheduledTopicState {
                    track_next_clone: AtomicBool::new(false),
                    dropping,
                    release_drop: std::sync::Mutex::new(Some(drop_release)),
                });
                let (manager, id) = runtime.block_on(async {
                    let manager = SessionManager::new(
                        LEASE_DURATION,
                        UnscheduledTopicFactory {
                            shared: shared.clone(),
                            block_drop: false,
                        },
                    );
                    let id = manager
                        .create("cluster-a", DecryptedClusterConfig::plaintext("kafka:9092"))
                        .await
                        .unwrap();
                    (manager, id)
                });

                // Occupy both workers before polling HTTP on the calling thread. This
                // guarantees the owned task cannot run until close has marked it closed.
                let mut worker_releases = Vec::new();
                let (blocked, worker_started) = std::sync::mpsc::channel();
                for _ in 0..2 {
                    let (release, wait) = std::sync::mpsc::channel();
                    worker_releases.push(release);
                    let blocked = blocked.clone();
                    runtime.spawn(async move {
                        let _ = blocked.send(());
                        let _ = wait.recv();
                    });
                }
                for _ in 0..2 {
                    worker_started.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                shared.track_next_clone.store(true, Ordering::SeqCst);
                let mut request = Some(Box::pin(manager.create_topic(
                    &id,
                    CreateTopicInput {
                        name: "orders".into(),
                        partitions: 1,
                        replication_factor: None,
                        configs: Default::default(),
                    },
                )));
                runtime.block_on(async {
                    assert!(
                        poll_once(request.as_mut().unwrap().as_mut())
                            .await
                            .is_none()
                    );
                });
                let mut closing = Box::pin(async {
                    match mode {
                        0 => manager.close(&id).await,
                        1 => {
                            manager.close_for_cluster("cluster-a").await;
                            Ok(())
                        }
                        _ => {
                            manager.close_all().await;
                            Ok(())
                        }
                    }
                });
                runtime.block_on(async {
                    assert!(poll_once(closing.as_mut()).await.is_none());
                });
                if cancel_caller {
                    drop(request.take());
                }
                for release in worker_releases {
                    release.send(()).unwrap();
                }
                drop_started.recv_timeout(Duration::from_secs(5)).unwrap();
                let returned_early = runtime.block_on(async {
                    tokio::time::timeout(Duration::from_millis(200), closing.as_mut())
                        .await
                        .is_ok()
                });
                // Always release the blocked destructor before asserting or exiting.
                release_drop.send(()).unwrap();
                if !returned_early {
                    runtime.block_on(closing).unwrap();
                }
                if let Some(request) = request {
                    assert!(matches!(
                        runtime.block_on(request),
                        Err(TopicOperationError::Session(SessionError::Expired))
                    ));
                }
                assert!(
                    !returned_early,
                    "close returned before owned task destruction: mode={mode} cancel={cancel_caller}"
                );
            }
        }
    }
}
