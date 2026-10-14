use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, CommitMode, Consumer},
    producer::{FutureProducer, FutureRecord},
};
use std::{net::SocketAddr, time::Duration};

pub fn validate_target(broker: &str, permit: &str) -> Result<(), &'static str> {
    let address: SocketAddr = broker
        .parse()
        .map_err(|_| "fixture requires one loopback address")?;
    if permit != "disposable-compose" || !address.ip().is_loopback() || address.port() == 0 {
        return Err(
            "fixture requires explicit disposable Compose authorization and a loopback port",
        );
    }
    Ok(())
}

pub async fn seed() {
    let broker = std::env::var("E2E_KAFKA_BROKERS").expect("missing fixture broker");
    let permit = std::env::var("E2E_KAFKA_WRITE_PERMIT").unwrap_or_default();
    validate_target(&broker, &permit).expect("unsafe Kafka fixture target");
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
