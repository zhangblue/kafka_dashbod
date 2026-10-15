mod support;

use kafka_dashboard::{
    api::{AppState, router_with_state},
    db,
    domain::cluster::DecryptedClusterConfig,
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

#[derive(Clone)]
struct Factory {
    creates: Arc<AtomicUsize>,
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
    async fn create(&self, config: DecryptedClusterConfig) -> Result<Client, SessionError> {
        assert_eq!(config.bootstrap_servers, "broker:9092");
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Client(self.drops.clone()))
    }
}

async fn app() -> (axum::Router, SessionManager<Factory>, Factory) {
    let factory = Factory {
        creates: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let sessions = SessionManager::new(Duration::from_secs(15), factory.clone());
    let state = AppState::new(db::connect("sqlite::memory:").await.unwrap())
        .with_session_manager(sessions.clone());
    (router_with_state(state), sessions, factory)
}

// Configuration requests must never create Kafka resources; session POST owns that boundary.
#[tokio::test]
async fn sessions_open_only_on_request_and_delete_releases_once() {
    let (app, sessions, factory) = app().await;
    let cluster = create_cluster(&app, "prod").await;
    request(&app, "GET", "/api/clusters", None).await;
    assert_eq!(factory.creates.load(Ordering::SeqCst), 0);
    let response = request(
        &app,
        "POST",
        &format!("/api/clusters/{cluster}/sessions"),
        None,
    )
    .await;
    assert_eq!(response.status(), 201);
    let created = response_json(response).await;
    assert_eq!(created["leaseSeconds"], 15);
    assert_eq!(created.as_object().unwrap().len(), 2);
    let id = created["sessionId"].as_str().unwrap();
    uuid::Uuid::parse_str(id).unwrap();
    assert_eq!(sessions.get(id).await.unwrap().cluster_id, cluster);
    assert_eq!(factory.creates.load(Ordering::SeqCst), 1);
    assert_eq!(
        request(&app, "DELETE", &format!("/api/sessions/{id}"), None)
            .await
            .status(),
        204
    );
    assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
    let response = request(&app, "DELETE", &format!("/api/sessions/{id}"), None).await;
    assert_eq!(response.status(), 410);
    let error = response_json(response).await;
    assert_eq!(error["code"], "SESSION_EXPIRED");
    assert!(error["requestId"].is_string());
    assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
}

// Missing clusters must not create an orphan Kafka client; stale session IDs are recognizable.
#[tokio::test]
async fn sessions_missing_cluster_and_unknown_session_return_stable_errors() {
    let (app, _, factory) = app().await;
    let response = request(&app, "POST", "/api/clusters/missing/sessions", None).await;
    assert_eq!(response.status(), 404);
    assert_eq!(response_json(response).await["code"], "CLUSTER_NOT_FOUND");
    assert_eq!(factory.creates.load(Ordering::SeqCst), 0);
    let response = request(&app, "DELETE", "/api/sessions/missing", None).await;
    assert_eq!(response.status(), 410);
    assert_eq!(response_json(response).await["code"], "SESSION_EXPIRED");
}

// Cluster deletion must drop only its sessions before deleting its saved configuration.
#[tokio::test]
async fn sessions_cluster_deletion_and_shutdown_release_all_resources() {
    let (app, sessions, factory) = app().await;
    let first = create_cluster(&app, "first").await;
    let second = create_cluster(&app, "second").await;
    let mut ids = Vec::new();
    for cluster in [&first, &first, &second] {
        let value = response_json(
            request(
                &app,
                "POST",
                &format!("/api/clusters/{cluster}/sessions"),
                None,
            )
            .await,
        )
        .await;
        ids.push(value["sessionId"].as_str().unwrap().to_owned());
    }
    let held = sessions.get(&ids[0]).await.unwrap();
    assert_eq!(
        request(&app, "DELETE", &format!("/api/clusters/{first}"), None)
            .await
            .status(),
        204
    );
    assert_eq!(factory.drops.load(Ordering::SeqCst), 2);
    assert!(held.clients.lock().await.is_none());
    assert!(matches!(
        sessions.get(&ids[0]).await,
        Err(SessionError::Expired)
    ));
    assert!(sessions.get(&ids[2]).await.is_ok());
    sessions.close_all().await;
    assert_eq!(factory.drops.load(Ordering::SeqCst), 3);
    assert!(matches!(
        sessions.get(&ids[2]).await,
        Err(SessionError::Expired)
    ));
    let response = request(
        &app,
        "POST",
        &format!("/api/clusters/{second}/sessions"),
        None,
    )
    .await;
    assert_eq!(response.status(), 503);
    assert_eq!(response_json(response).await["code"], "SHUTTING_DOWN");
}

// Even before the five-second reaper runs, an expired ID must never renew or collect again.
#[tokio::test]
async fn sessions_expiry_is_identifiable_through_api() {
    let (app, sessions, factory) = app().await;
    let cluster = create_cluster(&app, "prod").await;
    let value = response_json(
        request(
            &app,
            "POST",
            &format!("/api/clusters/{cluster}/sessions"),
            None,
        )
        .await,
    )
    .await;
    let id = value["sessionId"].as_str().unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(16)).await;
    sessions.reap_expired().await;
    let response = request(&app, "DELETE", &format!("/api/sessions/{id}"), None).await;
    assert_eq!(response.status(), 410);
    assert_eq!(response_json(response).await["code"], "SESSION_EXPIRED");
    assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
}
