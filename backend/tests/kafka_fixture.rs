#[path = "support/kafka_fixture.rs"]
mod kafka_fixture;

#[test]
fn fixture_refuses_non_disposable_or_non_loopback_targets() {
    assert!(kafka_fixture::validate_target("127.0.0.1:12345", "disposable-compose").is_ok());
    for (broker, permit) in [
        ("broker:9092", "disposable-compose"),
        ("127.0.0.1:9092,other:9092", "disposable-compose"),
        ("127.0.0.1:0", "disposable-compose"),
        ("127.0.0.2:12345", "disposable-compose"),
        ("[::1]:12345", "disposable-compose"),
        ("127.0.0.1:12345", ""),
    ] {
        assert!(kafka_fixture::validate_target(broker, permit).is_err());
    }
}

#[tokio::test]
#[ignore = "manages only a unique disposable Topic; run scripts/test-e2e.sh"]
async fn manage_disposable_topic() {
    kafka_fixture::verify_topic_management().await;
}

#[tokio::test]
#[ignore = "writes only to disposable Compose Kafka; run scripts/test-e2e.sh"]
async fn seed_disposable_kafka() {
    kafka_fixture::seed().await;
}
