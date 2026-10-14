mod support;

use kafka_dashboard::{
    api::{AppState, router_with_state},
    crypto::SecretCipher,
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
    let state = AppState::new(
        db::connect("sqlite::memory:").await.unwrap(),
        SecretCipher::from_key([7; 32]),
    )
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
    sessions.close_all().await;
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
        for p in &actual.partitions {
            assert!(p.low >= 0 && p.high >= p.low);
            assert_eq!(
                p.latest_message_offset,
                (p.high > p.low).then(|| p.high - 1)
            );
        }
    }
    for group in expected_groups {
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
                p.committed_offset
                    .map(|offset| p.high.saturating_sub(offset).max(0) as u64)
            );
        }
    }
    assert!(
        snapshot.partial_failures.is_empty(),
        "broker queries returned partial failures"
    );
}
