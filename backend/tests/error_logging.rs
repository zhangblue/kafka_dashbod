mod support;

use axum::{body::Body, http::Request};
use kafka_dashboard::{
    api::{AppState, router_with_state},
    db,
    domain::{
        cluster::DecryptedClusterConfig,
        topic_admin::{
            CreateTopicInput, TopicAdminError, TopicConfigUpdateOutcome, TopicConfigValue,
            UpdateTopicConfigsInput,
        },
    },
    sessions::manager::{SessionClientFactory, SessionContext, SessionError, SessionManager},
};
use rdkafka::{client::ClientContext, config::RDKafkaLogLevel, error::KafkaError};
use serde_json::json;
use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
use tower::ServiceExt;

#[derive(Clone)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);
impl Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct NativeErrorFactory;

fn native_failure() -> TopicAdminError {
    let raw =
        "never-log-native broker=never-log-broker password=never-log-password retention.ms=1234567";
    // The real native callback boundary must suppress even messages containing request values.
    SessionContext.error(KafkaError::AdminOpCreation(raw.into()), raw);
    SessionContext.log(RDKafkaLogLevel::Error, "never-log-native", raw);
    TopicAdminError::Authorization
}

impl SessionClientFactory for NativeErrorFactory {
    type Client = ();

    async fn create(&self, _: DecryptedClusterConfig) -> Result<(), SessionError> {
        Ok(())
    }

    async fn create_topic(&self, _: &mut (), _: &CreateTopicInput) -> Result<(), TopicAdminError> {
        Err(native_failure())
    }

    async fn describe_topic_configs(
        &self,
        _: &mut (),
        _: &str,
    ) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
        Err(native_failure())
    }

    async fn update_topic_configs(
        &self,
        _: &mut (),
        _: &str,
        _: &UpdateTopicConfigsInput,
    ) -> Result<TopicConfigUpdateOutcome, TopicAdminError> {
        Err(native_failure())
    }

    async fn delete_topic(&self, _: &mut (), _: &str) -> Result<(), TopicAdminError> {
        Err(native_failure())
    }
}

// Administration errors must correlate to the cluster without recording native text or payloads.
#[tokio::test]
async fn topic_failures_log_only_stable_context_on_every_route() {
    let sessions = SessionManager::new(Duration::from_secs(15), NativeErrorFactory);
    let state =
        AppState::new(db::connect("sqlite::memory:").await.unwrap()).with_session_manager(sessions);
    let app = router_with_state(state);
    let cluster = create_cluster(&app, "topic-logging").await;
    let created = response_json(
        request(
            &app,
            "POST",
            &format!("/api/clusters/{cluster}/sessions"),
            None,
        )
        .await,
    )
    .await;
    let session = created["sessionId"].as_str().unwrap();
    let logs = LogBuffer(Arc::new(Mutex::new(Vec::new())));
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let base = format!("/api/sessions/{session}/topics");
    for (method, path, payload, route) in [
        (
            "POST",
            base.clone(),
            json!({"name":"never-log-topic","configs":{"retention.ms":"1234567"}}),
            "/api/sessions/{id}/topics",
        ),
        (
            "GET",
            format!("{base}/never-log-topic/configs"),
            json!(null),
            "/api/sessions/{id}/topics/{topic}/configs",
        ),
        (
            "PUT",
            format!("{base}/never-log-topic/configs"),
            json!({"changes":{"retention.ms":"1234567"},"expectedValues":{"retention.ms":"7654321"}}),
            "/api/sessions/{id}/topics/{topic}/configs",
        ),
        (
            "DELETE",
            format!("{base}/never-log-topic"),
            json!({"confirmationName":"never-log-topic"}),
            "/api/sessions/{id}/topics/{topic}",
        ),
    ] {
        logs.0.lock().unwrap().clear();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(format!("{path}?password=never-log-query"))
                    .header("content-type", "application/json")
                    .header("x-request-id", "topic-logging-regression")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
        let body = response_json(response).await;
        assert_eq!(body["code"], "TOPIC_AUTHORIZATION_FAILED");
        assert_eq!(body["requestId"], "topic-logging-regression");
        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("code=\"TOPIC_AUTHORIZATION_FAILED\""),
            "{output}"
        );
        assert!(
            output.contains("request_id=topic-logging-regression"),
            "{output}"
        );
        assert!(
            output.contains(&format!("cluster_id={cluster}")),
            "{output}"
        );
        assert!(
            output.contains(&format!("operation=\"{method} {route}\"")),
            "{output}"
        );
        for sensitive in [
            "never-log-native",
            "never-log-broker",
            "never-log-password",
            "never-log-query",
            "never-log-topic",
            "1234567",
            "7654321",
            "retention.ms",
            session,
        ] {
            assert!(!output.contains(sensitive), "log contains {sensitive}");
            assert!(
                !body.to_string().contains(sensitive),
                "response contains {sensitive}"
            );
        }
    }
}

// Logging before middleware context is available loses correlation and cluster identity.
#[tokio::test]
async fn failed_requests_log_request_cluster_operation_without_secrets_or_query() {
    let test = test_app().await;
    let id = create_cluster(&test.app, "logging").await;
    let logs = LogBuffer(Arc::new(Mutex::new(Vec::new())));
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let response = test
        .app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/clusters/{id}?password=never-log-query"))
                .header("content-type", "application/json")
                .header("x-request-id", "logging-regression")
                .body(Body::from(
                    r#"{"password":"never-log-password","bootstrapServers":"never-log-broker"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    assert_eq!(
        response_json(response).await["requestId"],
        "logging-regression"
    );
    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(output.contains("request_id=logging-regression"), "{output}");
    assert!(output.contains(&format!("cluster_id={id}")), "{output}");
    assert!(
        output.contains("operation=\"PUT /api/clusters/{id}\""),
        "{output}"
    );
    for secret in ["never-log-query", "never-log-password", "never-log-broker"] {
        assert!(!output.contains(secret));
    }
}
