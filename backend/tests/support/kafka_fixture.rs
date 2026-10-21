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

fn authorized_broker() -> String {
    let broker = std::env::var("E2E_KAFKA_BROKERS").expect("missing fixture broker");
    let permit = std::env::var("E2E_KAFKA_FIXTURE_PERMIT").unwrap_or_default();
    validate_target(&broker, &permit).expect("unsafe Kafka fixture target");
    broker
}

pub async fn seed() {
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
        Self {
            admin,
            name,
            armed: true,
        }
    }

    async fn delete(mut self) -> String {
        self.admin
            .delete(&self.name)
            .await
            .expect("fixture Topic deletion failed");
        self.armed = false;
        self.name.clone()
    }
}

impl Drop for ManagedTopic {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let admin = self.admin.clone();
        let name = self.name.clone();
        // Drop must also run while unwinding an async test. Join a separate
        // runtime instead of detaching cleanup or nesting block_on in Tokio.
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

fn setting<'a>(entries: &'a [TopicConfigValue], key: &str) -> &'a TopicConfigValue {
    entries
        .iter()
        .find(|entry| entry.key == key)
        .expect("missing Kafka configuration")
}

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
pub async fn verify_topic_management() {
    let broker = authorized_broker();
    let native = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create_with_context::<_, AdminClient<_>>(SessionContext)
        .unwrap_or_else(|_| panic!("fixture client creation failed"));
    let admin = Arc::new(TopicAdminClient::new(
        Arc::new(native),
        Duration::from_secs(10),
    ));
    let guard = ManagedTopic::create(admin.clone()).await;
    let before = admin.describe(&guard.name).await.unwrap();
    assert_eq!(before.len(), 8);
    let original = setting(&before, "retention.ms");
    assert!(original.value.is_some());
    assert_eq!(original.source, TopicConfigSource::BrokerDefault);
    assert!(!original.read_only);
    for value in [Some("3600000".to_owned()), None] {
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
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        std::panic::resume_unwind(Box::new("synthetic fixture assertion failure"));
    }));
    assert!(unwind.is_err());
    assert_deleted(&admin, &abandoned).await;
}
