use kafka_dashboard::{
    domain::topic_admin::{
        CreateTopicInput, TopicAdminError, TopicConfigSource, TopicConfigValue,
        UpdateTopicConfigsInput,
    },
    kafka::topic_admin::TopicAdminClient,
    sessions::manager::SessionContext,
};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, CommitMode, Consumer},
    producer::{FutureProducer, FutureRecord},
};
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

// 在任何 fixture 写入前严格校验一次性授权、IPv4 回环地址与非零端口。
pub fn validate_target(broker: &str, permit: &str) -> Result<(), &'static str> {
    let address: SocketAddr = broker
        .parse()
        .map_err(|_| "fixture requires one loopback address")?;
    if permit != "disposable-compose" || address.ip() != Ipv4Addr::LOCALHOST || address.port() == 0
    {
        return Err(
            "fixture requires explicit disposable Compose authorization and a loopback port",
        );
    }
    Ok(())
}

// 在原生客户端创建前读取并校验显式写入许可，不接受任意已有集群目标。
fn authorized_broker() -> String {
    let broker = std::env::var("E2E_KAFKA_BROKERS").expect("missing fixture broker");
    let permit = std::env::var("E2E_KAFKA_FIXTURE_PERMIT").unwrap_or_default();
    validate_target(&broker, &permit).expect("unsafe Kafka fixture target");
    broker
}

// 在授权的一次性容器准备阶段创建种子 Topic、写入消息并提交独立已知位置。
pub async fn seed() {
    // 必须先验证一次性写入授权，再创建原生客户端；不能把 fixture 指向已有集群。
    let broker = authorized_broker();
    let topic = "dashboard-e2e-orders";
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "10000");
    let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, 2, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(20))),
        )
        .await
        .unwrap();
    assert!(
        results.iter().all(Result::is_ok),
        "fixture requires a fresh Kafka container"
    );
    // 只有确认新 Topic 创建成功后才生产消息，固定水位供只读验收独立比较。
    let producer: FutureProducer = config.create().unwrap();
    for offset in 0..10 {
        let payload = format!("fixture-{offset}");
        let delivery = producer
            .send(
                FutureRecord::to(topic)
                    .partition(0)
                    .key("fixture")
                    .payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(delivery.offset, offset);
    }
    let consumer: BaseConsumer = config
        .set("group.id", "dashboard-e2e-reader")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()
        .unwrap();
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset(topic, 0, Offset::Offset(4))
        .unwrap();
    offsets
        .add_partition_offset(topic, 1, Offset::Offset(0))
        .unwrap();
    // 提交仅属于获授权的一次性 fixture 准备，随后回读校验独立预期；产品路径不提交 Offset。
    consumer.commit(&offsets, CommitMode::Sync).unwrap();
    let committed = consumer
        .committed_offsets(offsets, Duration::from_secs(10))
        .unwrap();
    assert_eq!(
        committed.find_partition(topic, 0).unwrap().offset(),
        Offset::Offset(4)
    );
    assert_eq!(
        committed.find_partition(topic, 1).unwrap().offset(),
        Offset::Offset(0)
    );
    assert_eq!(
        consumer
            .fetch_watermarks(topic, 0, Duration::from_secs(10))
            .unwrap(),
        (0, 10)
    );
    assert_eq!(
        consumer
            .fetch_watermarks(topic, 1, Duration::from_secs(10))
            .unwrap(),
        (0, 0)
    );
}

// Only a successfully created, randomly named Topic can arm this guard. The seed
// Topic and resources supplied by callers can never become cleanup targets.
struct ManagedTopic {
    admin: Arc<TopicAdminClient>,
    name: String,
    armed: bool,
}

impl ManagedTopic {
    // 创建随机唯一 Topic 成功后才激活清理 guard，禁止把已有资源纳入删除目标。
    async fn create(admin: Arc<TopicAdminClient>) -> Self {
        let name = format!("dashboard-e2e-managed-fixture-{}", uuid::Uuid::new_v4());
        admin
            .create(&CreateTopicInput {
                name: name.clone(),
                partitions: 1,
                replication_factor: Some(1),
                configs: BTreeMap::from([("retention.bytes".into(), "104857600".into())]),
            })
            .await
            .expect("fixture Topic creation failed");
        // 创建成功后才持有已激活 guard，失败或既有资源绝不能成为清理目标。
        Self {
            admin,
            name,
            armed: true,
        }
    }

    // 在正常验收收尾阶段等待删除成功后解除 guard，失败时仍保留兜底清理。
    async fn delete(mut self) -> String {
        self.admin
            .delete(&self.name)
            .await
            .expect("fixture Topic deletion failed");
        // 等删除成功才解除兜底，删除失败时仍由 Drop 尝试回收本次资源。
        self.armed = false;
        self.name.clone()
    }
}

impl Drop for ManagedTopic {
    // 在异常展开阶段同步等待独立线程删除本次成功创建的 Topic，避免遗留清理任务。
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let admin = self.admin.clone();
        let name = self.name.clone();
        // Drop must also run while unwinding an async test. Join a separate
        // runtime instead of detaching cleanup or nesting block_on in Tokio.
        // 在独立线程创建运行时并 join，兼容异步测试展开且确保清理不会脱离测试后台运行。
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| ())?;
            runtime.block_on(admin.delete(&name)).map_err(|_| ())
        })
        .join();
        if !matches!(cleanup, Ok(Ok(()))) {
            eprintln!("Disposable Topic cleanup failed; Compose teardown is required");
            if !std::thread::panicking() {
                panic!("disposable Topic cleanup failed");
            }
        }
    }
}

// 在真实配置断言阶段按键查找 broker 返回项，缺项立即使验收失败。
fn setting<'a>(entries: &'a [TopicConfigValue], key: &str) -> &'a TopicConfigValue {
    entries
        .iter()
        .find(|entry| entry.key == key)
        .expect("missing Kafka configuration")
}

// 在删除及异常清理后有界重读，确认异步元数据传播完成并已不可见。
async fn assert_deleted(admin: &TopicAdminClient, name: &str) {
    for _ in 0..30 {
        if matches!(admin.describe(name).await, Err(TopicAdminError::NotFound)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("deleted fixture Topic remains visible");
}

// Catches incorrect native result decoding, SET/DELETE semantics and accidental
// replacement of unrelated configuration on a real, explicitly authorized broker.
// 在授权 broker 上验证真实增量 SET、DELETE 和来源解码，且保留无关配置。
pub async fn verify_topic_management() {
    // 必须先验证一次性写入授权，再创建原生客户端；不能把 fixture 指向已有集群。
    let broker = authorized_broker();
    let native = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create_with_context::<_, AdminClient<_>>(SessionContext)
        .unwrap_or_else(|_| panic!("fixture client creation failed"));
    let admin = Arc::new(TopicAdminClient::new(
        Arc::new(native),
        Duration::from_secs(10),
    ));
    // 仅让本次成功创建的随机 Topic 进入 guard；不删除种子 Topic 或调用方已有资源。
    let guard = ManagedTopic::create(admin.clone()).await;
    let before = admin.describe(&guard.name).await.unwrap();
    assert_eq!(before.len(), 8);
    let original = setting(&before, "retention.ms");
    assert!(original.value.is_some());
    assert_eq!(original.source, TopicConfigSource::BrokerDefault);
    assert!(!original.read_only);
    for value in [Some("3600000".to_owned()), None] {
        // 每次修改前重新读取预期值，再增量写入并回读来源，覆盖 SET 与恢复默认两条路径。
        let current = admin.describe(&guard.name).await.unwrap();
        admin
            .update(
                &guard.name,
                &UpdateTopicConfigsInput {
                    changes: BTreeMap::from([("retention.ms".into(), value.clone())]),
                    expected_values: BTreeMap::from([(
                        "retention.ms".into(),
                        setting(&current, "retention.ms").value.clone(),
                    )]),
                },
            )
            .await
            .unwrap();
        let reread = admin.describe(&guard.name).await.unwrap();
        let retention = setting(&reread, "retention.ms");
        assert_eq!(
            retention.value,
            value.clone().or_else(|| original.value.clone())
        );
        assert_eq!(
            retention.source,
            if value.is_some() {
                TopicConfigSource::TopicOverride
            } else {
                TopicConfigSource::BrokerDefault
            }
        );
        let untouched = setting(&reread, "retention.bytes");
        assert_eq!(untouched.value.as_deref(), Some("104857600"));
        assert_eq!(untouched.source, TopicConfigSource::TopicOverride);
    }
    let deleted = guard.delete().await;
    assert_deleted(&admin, &deleted).await;

    // Exercise the actual unwind guard, so an assertion failure cannot silently
    // leave its resource behind until the whole Compose environment is removed.
    let guard = ManagedTopic::create(admin.clone()).await;
    let abandoned = guard.name.clone();
    // 主动模拟断言展开并等待 guard 清理后再确认删除，证明失败路径也不遗留 Topic。
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        std::panic::resume_unwind(Box::new("synthetic fixture assertion failure"));
    }));
    assert!(unwind.is_err());
    assert_deleted(&admin, &abandoned).await;
}
