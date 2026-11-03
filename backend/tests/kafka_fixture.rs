#[path = "support/kafka_fixture.rs"]
mod kafka_fixture;

// 验证写入 fixture 拒绝非一次性授权、非回环地址及零端口，保护已有集群。
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

// 仅在显式启用的一次性 broker 验收中检查 Topic 管理和失败清理。
#[tokio::test]
#[ignore = "manages only a unique disposable Topic; run scripts/test-e2e.sh"]
async fn manage_disposable_topic() {
    kafka_fixture::verify_topic_management().await;
}

// 仅在显式启用的一次性 broker 准备阶段写入固定水位与提交位置。
#[tokio::test]
#[ignore = "writes only to disposable Compose Kafka; run scripts/test-e2e.sh"]
async fn seed_disposable_kafka() {
    kafka_fixture::seed().await;
}
