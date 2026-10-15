mod support;

use kafka_dashboard::{
    api::{AppState, router_with_state},
    db,
    domain::{cluster::DecryptedClusterConfig, snapshot::ClusterSnapshot},
    sessions::manager::{SessionClientFactory, SessionError, SessionManager},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::Notify;

#[derive(Clone)]
struct Factory {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    drops: Arc<AtomicUsize>,
}
struct Client(Arc<AtomicUsize>);
impl Drop for Client {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl SessionClientFactory for Factory {
    type Client = Client;
    async fn create(&self, _: DecryptedClusterConfig) -> Result<Client, SessionError> {
        Ok(Client(self.drops.clone()))
    }
    async fn snapshot(
        &self,
        _: &mut Client,
        cluster_id: &str,
    ) -> Result<ClusterSnapshot, SessionError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(ClusterSnapshot {
            cluster_id: cluster_id.into(),
            collected_at: "2026-10-15T00:00:00Z".into(),
            topics: vec![],
            consumer_group_ids: vec![],
            consumer_groups: vec![],
            partial_failures: vec![],
        })
    }
}

// Missing renewal, allowing overlap, or dropping the client while work runs breaks this contract.
#[tokio::test]
async fn snapshot_api_renews_rejects_overlap_and_close_waits_after_http_cancellation() {
    let factory = Factory {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let sessions = SessionManager::new(Duration::from_secs(15), factory.clone());
    let state = AppState::new(db::connect("sqlite::memory:").await.unwrap())
        .with_session_manager(sessions.clone());
    let app = router_with_state(state);
    let cluster_id = create_cluster(&app, "snapshot").await;
    let created = response_json(
        request(
            &app,
            "POST",
            &format!("/api/clusters/{cluster_id}/sessions"),
            None,
        )
        .await,
    )
    .await;
    let id = created["sessionId"].as_str().unwrap().to_owned();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(10)).await;
    let uri = format!("/api/sessions/{id}/snapshot");
    let first_app = app.clone();
    let first_uri = uri.clone();
    let first = tokio::spawn(async move { request(&first_app, "GET", &first_uri, None).await });
    factory.entered.notified().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(
        sessions.get(&id).await.is_ok(),
        "snapshot must renew the lease"
    );
    let conflict = request(&app, "GET", &uri, None).await;
    assert_eq!(conflict.status(), 409);
    assert_eq!(
        response_json(conflict).await["code"],
        "SNAPSHOT_IN_PROGRESS"
    );
    first.abort();
    let _ = first.await;
    let closing_sessions = sessions.clone();
    let closing_id = id.clone();
    let closing = tokio::spawn(async move { closing_sessions.close(&closing_id).await });
    tokio::task::yield_now().await;
    assert!(!closing.is_finished());
    assert_eq!(factory.drops.load(Ordering::SeqCst), 0);
    factory.release.notify_one();
    closing.await.unwrap().unwrap();
    assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
    assert_eq!(request(&app, "GET", &uri, None).await.status(), 410);
    let created = response_json(
        request(
            &app,
            "POST",
            &format!("/api/clusters/{cluster_id}/sessions"),
            None,
        )
        .await,
    )
    .await;
    let next_id = created["sessionId"].as_str().unwrap();
    factory.release.notify_one();
    let response = request(
        &app,
        "GET",
        &format!("/api/sessions/{next_id}/snapshot"),
        None,
    )
    .await;
    assert_eq!(response.status(), 200);
    let snapshot = response_json(response).await;
    assert_eq!(snapshot["clusterId"], cluster_id);
    assert_eq!(snapshot["partialFailures"], serde_json::json!([]));
    assert!(snapshot["consumerGroups"].is_array());
    assert!(snapshot["consumerGroupIds"].is_array());
    sessions.close_all().await;
}

#[test]
fn known_committed_fixture_requires_complete_and_valid_environment_values() {
    let values = |group: Option<&str>,
                  topic: Option<&str>,
                  partition: Option<&str>,
                  offset: Option<&str>| {
        [group, topic, partition, offset].map(|value| value.map(str::to_owned))
    };
    assert!(
        known_offset_fixture([None, None, None, None])
            .unwrap()
            .is_none()
    );
    assert!(known_offset_fixture(values(Some("reader"), None, None, None)).is_err());
    assert!(
        known_offset_fixture(values(
            Some("reader"),
            Some("orders"),
            Some("bad"),
            Some("42")
        ))
        .is_err()
    );
    assert!(
        known_offset_fixture(values(
            Some("reader"),
            Some("orders"),
            Some("-1"),
            Some("42")
        ))
        .is_err()
    );
    assert!(
        known_offset_fixture(values(
            Some("reader"),
            Some("orders"),
            Some("0"),
            Some("-1")
        ))
        .is_err()
    );
    let expected = known_offset_fixture(values(
        Some("reader"),
        Some("orders"),
        Some("2"),
        Some("42"),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        (
            expected.group_id.as_str(),
            expected.topic.as_str(),
            expected.partition,
            expected.committed
        ),
        ("reader", "orders", 2, Some(42))
    );
    assert_eq!(
        known_offset_fixture(values(
            Some("reader"),
            Some("orders"),
            Some("2"),
            Some("null")
        ))
        .unwrap()
        .unwrap()
        .committed,
        None
    );
}

struct KnownOffset {
    group_id: String,
    topic: String,
    partition: i32,
    committed: Option<i64>,
}

fn known_offset_fixture(values: [Option<String>; 4]) -> Result<Option<KnownOffset>, &'static str> {
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    let [Some(group_id), Some(topic), Some(partition), Some(offset)] = values else {
        return Err("set all four TEST_KAFKA_GROUP_ID/TOPIC/PARTITION/COMMITTED_OFFSET values");
    };
    let partition: i32 = partition
        .parse()
        .map_err(|_| "invalid expected partition")?;
    let committed = if offset == "null" {
        None
    } else {
        Some(
            offset
                .parse::<i64>()
                .map_err(|_| "invalid expected committed offset")?,
        )
    };
    if group_id.is_empty()
        || topic.is_empty()
        || partition < 0
        || committed.is_some_and(|value| value < 0)
    {
        return Err("expected group/topic must be nonempty and partition/offset nonnegative");
    }
    Ok(Some(KnownOffset {
        group_id,
        topic,
        partition,
        committed,
    }))
}

// Read-only integration: never create topics, produce, subscribe, or commit offsets.
#[tokio::test]
#[ignore = "requires an explicitly authorized TEST_KAFKA_BROKERS"]
async fn readonly_broker_snapshot_contains_all_discoverable_resources() {
    use kafka_dashboard::{
        kafka::collector::{CollectionOptions, KafkaProbe, NativeProbe, collect},
        sessions::manager::KafkaSessionClientFactory,
    };
    let brokers = std::env::var("TEST_KAFKA_BROKERS").expect("set TEST_KAFKA_BROKERS");
    let known = known_offset_fixture(
        [
            "TEST_KAFKA_GROUP_ID",
            "TEST_KAFKA_TOPIC",
            "TEST_KAFKA_PARTITION",
            "TEST_KAFKA_COMMITTED_OFFSET",
        ]
        .map(|key| std::env::var(key).ok()),
    )
    .unwrap();
    let factory = KafkaSessionClientFactory;
    let client = factory
        .create(DecryptedClusterConfig::plaintext(brokers))
        .await
        .unwrap();
    let options = CollectionOptions::default();
    let probe = NativeProbe::new(&client, options);
    let expected_topics = probe.topics().await.unwrap();
    let expected_groups = probe.groups().await.unwrap();
    let snapshot = collect(&probe, "readonly", options).await;
    probe.finish().await;
    assert!(!expected_topics.is_empty());
    assert_eq!(snapshot.topics.len(), expected_topics.len());
    for topic in expected_topics {
        let actual = snapshot
            .topics
            .iter()
            .find(|row| row.topic == topic.topic)
            .unwrap();
        assert_eq!(actual.partitions.len(), topic.partitions.len());
        assert!(!actual.incomplete);
        for p in &actual.partitions {
            let low = p.low.expect("successful watermark must have low");
            let high = p.high.expect("successful watermark must have high");
            assert!(low >= 0 && high >= low);
            assert_eq!(p.latest_message_offset, (high > low).then(|| high - 1));
        }
    }
    for group in expected_groups {
        assert!(snapshot.consumer_group_ids.contains(&group.group_id));
        assert!(
            snapshot
                .consumer_groups
                .iter()
                .any(|row| row.group_id == group.group_id)
        );
        let first = client
            .group_consumers
            .get_or_create(&group.group_id, &client.config)
            .unwrap();
        let second = client
            .group_consumers
            .get_or_create(&group.group_id, &client.config)
            .unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "group readers must be reused within a session"
        );
    }
    for group in &snapshot.consumer_groups {
        for p in &group.partitions {
            assert_eq!(
                p.lag,
                p.high
                    .zip(p.committed_offset)
                    .map(|(high, offset)| high.saturating_sub(offset).max(0) as u64)
            );
        }
    }
    if let Some(known) = known {
        let group = snapshot
            .consumer_groups
            .iter()
            .find(|row| row.group_id == known.group_id && row.topic == known.topic)
            .expect("configured group/topic must appear in snapshot");
        let partition = group
            .partitions
            .iter()
            .find(|row| row.partition == known.partition)
            .expect("configured partition must appear in snapshot");
        assert_eq!(
            partition.committed_offset, known.committed,
            "broker committed offset must match configured independent expectation"
        );
    }
    assert!(
        snapshot.partial_failures.is_empty(),
        "broker queries returned partial failures"
    );
}
