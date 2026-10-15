mod support;

use axum::{
    body::Body,
    http::{Request, header::CONTENT_TYPE},
};
use kafka_dashboard::{
    api::{AppState, router_with_state},
    db,
    domain::cluster::DecryptedClusterConfig,
    kafka::client::ConnectionTestResult,
    sessions::manager::{SessionClientFactory, SessionError, SessionManager},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
use tower::ServiceExt;

#[derive(Clone, Default)]
struct Factory(Arc<Mutex<Vec<DecryptedClusterConfig>>>);
impl SessionClientFactory for Factory {
    type Client = ();
    async fn create(&self, _: DecryptedClusterConfig) -> Result<(), SessionError> {
        panic!("draft testing must not create a monitoring session")
    }
    async fn test_connection(
        &self,
        config: DecryptedClusterConfig,
    ) -> Result<ConnectionTestResult, SessionError> {
        self.0.lock().unwrap().push(config);
        Ok(ConnectionTestResult {
            broker_count: 2,
            topic_count: 3,
            elapsed_ms: 4,
        })
    }
}

async fn app() -> (TestApp, Factory) {
    let factory = Factory::default();
    let pool = db::connect("sqlite::memory:").await.unwrap();
    let state = AppState::new(pool.clone()).with_session_manager(SessionManager::new(
        Duration::from_secs(15),
        factory.clone(),
    ));
    let clusters = state.clusters.clone();
    (
        TestApp {
            app: router_with_state(state),
            pool,
            clusters,
        },
        factory,
    )
}

fn secure(name: &str) -> Value {
    json!({
        "name": name, "bootstrapServers": "127.0.0.1:1", "securityProtocol": "SASL_SSL",
        "saslMechanism": "PLAIN", "username": "reader", "password": "fixture-password",
        "caPem": "fixture-ca", "clientCertificatePem": "fixture-certificate",
        "clientKeyPem": "fixture-key", "clientKeyPassphrase": "fixture-passphrase", "connectionTimeoutMs": 1
    })
}

// A new draft uses the submitted connection settings and never creates a saved cluster.
#[tokio::test]
async fn new_draft_test_is_manager_owned_and_does_not_persist() {
    let (ctx, factory) = app().await;
    let response = request(
        &ctx.app,
        "POST",
        "/api/clusters/test",
        Some(secure("draft")),
    )
    .await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response_json(response).await,
        json!({"brokerCount":2,"topicCount":3,"elapsedMs":4})
    );
    assert!(ctx.clusters.list().await.unwrap().is_empty());
    let configs = factory.0.lock().unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].bootstrap_servers, "127.0.0.1:1");
    assert_eq!(
        configs[0].client_key_passphrase.as_deref(),
        Some("fixture-passphrase")
    );
}

// Unsaved edits must change the test destination while omitted credentials reuse saved values.
#[tokio::test]
async fn existing_draft_merges_secrets_and_keeps_saved_configuration_unchanged() {
    let (ctx, factory) = app().await;
    let created =
        response_json(request(&ctx.app, "POST", "/api/clusters", Some(secure("saved"))).await)
            .await;
    let id = created["id"].as_str().unwrap();
    let mut draft = secure("unsaved");
    draft["bootstrapServers"] = json!("edited:9093");
    for field in [
        "password",
        "caPem",
        "clientCertificatePem",
        "clientKeyPem",
        "clientKeyPassphrase",
    ] {
        draft.as_object_mut().unwrap().remove(field);
    }
    let response = request(
        &ctx.app,
        "POST",
        &format!("/api/clusters/{id}/test"),
        Some(draft),
    )
    .await;
    assert_eq!(response.status(), 200);
    {
        let configs = factory.0.lock().unwrap();
        assert_eq!(configs.len(), 1);
        let tested = &configs[0];
        assert_eq!(tested.bootstrap_servers, "edited:9093");
        assert_eq!(tested.password.as_deref(), Some("fixture-password"));
        assert_eq!(tested.ca_pem.as_deref(), Some("fixture-ca"));
        assert_eq!(
            tested.client_certificate_pem.as_deref(),
            Some("fixture-certificate")
        );
        assert_eq!(tested.client_key_pem.as_deref(), Some("fixture-key"));
        assert_eq!(
            tested.client_key_passphrase.as_deref(),
            Some("fixture-passphrase")
        );
    }
    assert_eq!(
        serde_json::to_value(ctx.clusters.get(id).await.unwrap().unwrap()).unwrap(),
        created
    );
}

// Draft clear flags affect only the test; saved secrets survive and are used by a bodyless test.
#[tokio::test]
async fn draft_clear_is_temporary_and_bodyless_test_retains_compatibility() {
    let (ctx, factory) = app().await;
    let created =
        response_json(request(&ctx.app, "POST", "/api/clusters", Some(secure("saved"))).await)
            .await;
    let id = created["id"].as_str().unwrap();
    let mut draft = cluster("unsaved");
    for field in [
        "clearPassword",
        "clearCaPem",
        "clearClientCertificatePem",
        "clearClientKeyPem",
        "clearClientKeyPassphrase",
    ] {
        draft[field] = json!(true);
    }
    for body in [Some(draft), None] {
        assert_eq!(
            request(&ctx.app, "POST", &format!("/api/clusters/{id}/test"), body)
                .await
                .status(),
            200
        );
    }
    let configs = factory.0.lock().unwrap();
    assert_eq!(configs.len(), 2);
    assert!(configs[0].password.is_none() && configs[0].ca_pem.is_none());
    assert!(configs[0].client_certificate_pem.is_none() && configs[0].client_key_pem.is_none());
    assert!(configs[0].client_key_passphrase.is_none());
    assert_eq!(configs[1].password.as_deref(), Some("fixture-password"));
    assert_eq!(
        configs[1].client_key_passphrase.as_deref(),
        Some("fixture-passphrase")
    );
}

// Invalid drafts must fail before native work, and missing IDs must not fall back to new drafts.
#[tokio::test]
async fn invalid_drafts_are_rejected_without_tests_or_storage_changes() {
    let (ctx, factory) = app().await;
    let id = create_cluster(&ctx.app, "saved").await;
    for path in [
        "/api/clusters/test".to_owned(),
        format!("/api/clusters/{id}/test"),
    ] {
        let mut invalid = cluster("invalid");
        invalid["clientKeyPassphrase"] = json!("fixture-secret");
        let response = request(&ctx.app, "POST", &path, Some(invalid)).await;
        assert_eq!(response.status(), 422);
        assert!(!response_text(response).await.contains("fixture-secret"));
        let response = request(
            &ctx.app,
            "POST",
            &path,
            Some(json!({"name":"x".repeat(300_000)})),
        )
        .await;
        assert_eq!(response.status(), 413);
    }
    assert_eq!(
        request(
            &ctx.app,
            "POST",
            "/api/clusters/missing/test",
            Some(cluster("draft"))
        )
        .await
        .status(),
        404
    );
    assert_eq!(ctx.clusters.list().await.unwrap().len(), 1);
    assert!(factory.0.lock().unwrap().is_empty());
}

// Simple cross-origin bodies must not redirect a saved secret to an arbitrary broker.
#[tokio::test]
async fn draft_requires_json_content_type_before_reusing_saved_secrets() {
    let (ctx, factory) = app().await;
    let created =
        response_json(request(&ctx.app, "POST", "/api/clusters", Some(secure("saved"))).await)
            .await;
    let id = created["id"].as_str().unwrap();
    let draft = json!({
        "name":"unsaved", "bootstrapServers":"changed:9093", "securityProtocol":"SASL_SSL",
        "saslMechanism":"PLAIN", "username":"reader"
    });
    for content_type in [
        Some("text/plain"),
        None,
        Some("text/plain; charset=utf-8"),
        Some("application/x-www-form-urlencoded"),
        Some("text/json"),
        Some("application/jsonp"),
    ] {
        let mut builder = Request::post(format!("/api/clusters/{id}/test"));
        if let Some(value) = content_type {
            builder = builder.header(CONTENT_TYPE, value);
        }
        let response = ctx
            .app
            .clone()
            .oneshot(builder.body(Body::from(draft.to_string())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 422, "accepted {content_type:?}");
        assert_eq!(response_json(response).await["code"], "VALIDATION_ERROR");
        assert!(
            factory.0.lock().unwrap().is_empty(),
            "rejected content type reached connection testing"
        );
    }
    assert_eq!(
        serde_json::to_value(ctx.clusters.get(id).await.unwrap().unwrap()).unwrap(),
        created
    );
}

// The security check must retain standard JSON media types and legacy empty-body calls.
#[tokio::test]
async fn draft_accepts_json_media_types_and_empty_body_without_content_type() {
    let (ctx, factory) = app().await;
    let id = create_cluster(&ctx.app, "saved").await;
    for content_type in [
        "application/json",
        "application/json; charset=utf-8",
        "application/vnd.kafka+json",
    ] {
        let response = ctx
            .app
            .clone()
            .oneshot(
                Request::post(format!("/api/clusters/{id}/test"))
                    .header(CONTENT_TYPE, content_type)
                    .body(Body::from(cluster("unsaved").to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "rejected {content_type}");
    }
    let response = ctx
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/clusters/{id}/test"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(factory.0.lock().unwrap().len(), 4);
}
