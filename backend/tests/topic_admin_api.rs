mod support;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{Router, body::Body, http::Request};
use kafka_dashboard::{
    api::{AppState, router_with_state},
    db,
    domain::{
        cluster::DecryptedClusterConfig,
        topic_admin::{
            CreateTopicInput, TopicAdminError, TopicConfigSource, TopicConfigUpdateOutcome,
            TopicConfigValue, UpdateTopicConfigsInput,
        },
    },
    sessions::manager::{SessionClientFactory, SessionError, SessionManager},
};
use serde_json::{Value, json};
use support::*;
use tokio::sync::Mutex;
use tower::ServiceExt;

#[derive(Default)]
struct Calls {
    created: Vec<CreateTopicInput>,
    described: Vec<String>,
    updated: Vec<(String, UpdateTopicConfigsInput)>,
    deleted: Vec<String>,
}

#[derive(Clone, Default)]
struct Factory {
    calls: Arc<Mutex<Calls>>,
    error: Option<TopicAdminError>,
    unconfirmed: bool,
}

fn config_values() -> Vec<TopicConfigValue> {
    vec![
        TopicConfigValue {
            key: "retention.ms".into(),
            value: Some("60000".into()),
            source: TopicConfigSource::TopicOverride,
            read_only: false,
        },
        TopicConfigValue {
            key: "compression.type".into(),
            value: None,
            source: TopicConfigSource::Unknown,
            read_only: true,
        },
    ]
}

impl SessionClientFactory for Factory {
    type Client = ();

    async fn create(&self, _: DecryptedClusterConfig) -> Result<(), SessionError> {
        Ok(())
    }

    async fn create_topic(
        &self,
        _: &mut (),
        input: &CreateTopicInput,
    ) -> Result<(), TopicAdminError> {
        self.calls.lock().await.created.push(input.clone());
        self.error.clone().map_or(Ok(()), Err)
    }

    async fn describe_topic_configs(
        &self,
        _: &mut (),
        topic: &str,
    ) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
        self.calls.lock().await.described.push(topic.into());
        self.error.clone().map_or_else(|| Ok(config_values()), Err)
    }

    async fn update_topic_configs(
        &self,
        _: &mut (),
        topic: &str,
        input: &UpdateTopicConfigsInput,
    ) -> Result<TopicConfigUpdateOutcome, TopicAdminError> {
        self.calls
            .lock()
            .await
            .updated
            .push((topic.into(), input.clone()));
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(if self.unconfirmed {
            TopicConfigUpdateOutcome::AppliedButUnconfirmed
        } else {
            TopicConfigUpdateOutcome::Confirmed(config_values())
        })
    }

    async fn delete_topic(&self, _: &mut (), topic: &str) -> Result<(), TopicAdminError> {
        self.calls.lock().await.deleted.push(topic.into());
        self.error.clone().map_or(Ok(()), Err)
    }
}

async fn app(factory: Factory) -> (Router, String, SessionManager<Factory>) {
    let sessions = SessionManager::new(Duration::from_secs(15), factory);
    let state = AppState::new(db::connect("sqlite::memory:").await.unwrap())
        .with_session_manager(sessions.clone());
    let app = router_with_state(state);
    let cluster = create_cluster(&app, "topic-admin").await;
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
    (app, value["sessionId"].as_str().unwrap().into(), sessions)
}

fn update_input() -> Value {
    json!({"changes":{"retention.ms":null},"expectedValues":{"retention.ms":"60000"}})
}

// Missing validation or incorrect forwarding must never send a different creation to Kafka.
#[tokio::test]
async fn topic_routes_validate_and_forward_only_safe_values() {
    let factory = Factory::default();
    let calls = factory.calls.clone();
    let (app, id, _) = app(factory).await;
    let response = request(
        &app,
        "POST",
        &format!("/api/sessions/{id}/topics"),
        Some(
            json!({"name":"orders.v2","partitions":3,"replicationFactor":null,
            "configs":{"retention.ms":"3600000"}}),
        ),
    )
    .await;
    assert_eq!(response.status(), 201);
    assert_eq!(response_json(response).await, json!({"status":"created"}));
    let recorded = calls.lock().await;
    assert_eq!(recorded.created.len(), 1);
    assert_eq!(recorded.created[0].name, "orders.v2");
    assert_eq!(recorded.created[0].partitions, 3);
    assert_eq!(recorded.created[0].replication_factor, None);
    assert_eq!(
        recorded.created[0].configs,
        BTreeMap::from([("retention.ms".into(), "3600000".into())])
    );
    drop(recorded);

    let response = request(
        &app,
        "POST",
        &format!("/api/sessions/{id}/topics"),
        Some(json!({"name":"default-topic"})),
    )
    .await;
    assert_eq!(response.status(), 201);
    let recorded = calls.lock().await;
    assert_eq!(recorded.created[1].partitions, 1);
    assert!(recorded.created[1].configs.is_empty());
}

// Responses preserve Kafka's unknown/read-only metadata; reset-default remains a null change.
#[tokio::test]
async fn topic_configs_return_actual_values_and_preserve_incremental_reset() {
    let factory = Factory::default();
    let calls = factory.calls.clone();
    let (app, id, _) = app(factory).await;
    let uri = format!("/api/sessions/{id}/topics/orders.v2/configs");
    for (method, body) in [("GET", None), ("PUT", Some(update_input()))] {
        let response = request(&app, method, &uri, body).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response_json(response).await,
            json!({"configs":[
                {"key":"retention.ms","value":"60000","source":"topicOverride","readOnly":false},
                {"key":"compression.type","value":null,"source":"unknown","readOnly":true}
            ]})
        );
    }
    let recorded = calls.lock().await;
    assert_eq!(recorded.described, ["orders.v2"]);
    assert_eq!(recorded.updated[0].0, "orders.v2");
    assert_eq!(
        recorded.updated[0].1.changes,
        BTreeMap::from([("retention.ms".into(), None)])
    );
    assert_eq!(
        recorded.updated[0].1.expected_values,
        BTreeMap::from([("retention.ms".into(), Some("60000".into()))])
    );
}

// A successful alter followed by a failed read must not claim submitted values were confirmed.
#[tokio::test]
async fn applied_but_unconfirmed_has_fixed_accepted_response() {
    let (app, id, _) = app(Factory {
        unconfirmed: true,
        ..Factory::default()
    })
    .await;
    let response = request(
        &app,
        "PUT",
        &format!("/api/sessions/{id}/topics/orders/configs"),
        Some(update_input()),
    )
    .await;
    assert_eq!(response.status(), 202);
    assert_eq!(
        response_json(response).await,
        json!({"status":"appliedButUnconfirmed"})
    );
}

// Encoded session/topic segments must be decoded once before lookup, forwarding and confirmation.
#[tokio::test]
async fn encoded_paths_reach_the_exact_topic_and_delete_returns_an_empty_body() {
    let factory = Factory::default();
    let calls = factory.calls.clone();
    let (app, id, _) = app(factory).await;
    let encoded_id = format!("%{:02X}{}", id.as_bytes()[0], &id[1..]);
    let path = format!("/api/sessions/{encoded_id}/topics/orders%2Ev2");
    assert_eq!(
        request(&app, "GET", &format!("{path}/configs"), None)
            .await
            .status(),
        200
    );
    assert_eq!(
        request(
            &app,
            "PUT",
            &format!("{path}/configs"),
            Some(update_input())
        )
        .await
        .status(),
        200
    );
    let response = request(
        &app,
        "DELETE",
        &path,
        Some(json!({"confirmationName":"orders.v2"})),
    )
    .await;
    assert_eq!(response.status(), 204);
    assert!(response_text(response).await.is_empty());
    let recorded = calls.lock().await;
    assert_eq!(recorded.described, ["orders.v2"]);
    assert_eq!(recorded.updated[0].0, "orders.v2");
    assert_eq!(recorded.deleted, ["orders.v2"]);
}

// Frontend bypasses cannot submit invalid counts, unsupported keys, or missing compare values.
#[tokio::test]
async fn invalid_create_and_update_fields_are_rejected_before_native_work() {
    let factory = Factory::default();
    let calls = factory.calls.clone();
    let (app, id, _) = app(factory).await;
    let base = format!("/api/sessions/{id}/topics");
    let cases = [
        ("POST", base.clone(), json!({"name":"bad/name"}), "name"),
        (
            "POST",
            base.clone(),
            json!({"name":"orders","partitions":0}),
            "partitions",
        ),
        (
            "POST",
            base.clone(),
            json!({"name":"orders","replicationFactor":0}),
            "replicationFactor",
        ),
        (
            "POST",
            base.clone(),
            json!({"name":"orders","partitions":100001}),
            "partitions",
        ),
        (
            "POST",
            base.clone(),
            json!({"name":"orders","replicationFactor":10001}),
            "replicationFactor",
        ),
        (
            "POST",
            base.clone(),
            json!({"name":"orders","configs":{"ssl.password":"never-return-value"}}),
            "configs",
        ),
        (
            "PUT",
            format!("{base}/orders/configs"),
            json!({"changes":{"retention.ms":"never-return-value"},"expectedValues":{"retention.ms":null}}),
            "changes.retention.ms",
        ),
        (
            "PUT",
            format!("{base}/orders/configs"),
            json!({"changes":{"retention.ms":null},"expectedValues":{}}),
            "expectedValues.retention.ms",
        ),
        (
            "PUT",
            format!("{base}/orders/configs"),
            json!({"changes":{"ssl.password":null},"expectedValues":{"ssl.password":null}}),
            "changes",
        ),
    ];
    for (method, path, payload, field) in cases {
        let response = request(&app, method, &path, Some(payload)).await;
        assert_eq!(response.status(), 422, "{method} {path}");
        let body = response_json(response).await;
        assert_eq!(body["code"], "VALIDATION_ERROR");
        assert!(body["details"][field].is_string(), "{body}");
        assert!(!body.to_string().contains("never-return-value"));
        assert!(body["requestId"].is_string());
    }
    let recorded = calls.lock().await;
    assert!(recorded.created.is_empty());
    assert!(recorded.updated.is_empty());
}

// Decoding must not let separators, NULs, double encoding or protected internal names bypass checks.
#[tokio::test]
async fn invalid_topic_paths_and_delete_confirmations_never_reach_kafka() {
    let factory = Factory::default();
    let calls = factory.calls.clone();
    let (app, id, _) = app(factory).await;
    let base = format!("/api/sessions/{id}/topics");
    for topic in ["bad%2Fname", "bad%00name", "bad%252Fname", "%2E", "%2E%2E"] {
        for method in ["GET", "PUT", "DELETE"] {
            let (path, payload) = match method {
                "GET" => (format!("{base}/{topic}/configs"), None),
                "PUT" => (format!("{base}/{topic}/configs"), Some(update_input())),
                _ => (
                    format!("{base}/{topic}"),
                    Some(json!({"confirmationName":"bad/name"})),
                ),
            };
            let response = request(&app, method, &path, payload).await;
            assert_eq!(response.status(), 422, "{method} {topic}");
            assert_eq!(response_json(response).await["code"], "VALIDATION_ERROR");
        }
    }
    for confirmation in ["Orders", "orders ", "payments", "orders%2Ev2"] {
        let response = request(
            &app,
            "DELETE",
            &format!("{base}/orders"),
            Some(json!({"confirmationName":confirmation})),
        )
        .await;
        assert_eq!(response.status(), 422);
        assert!(response_json(response).await["details"]["confirmationName"].is_string());
    }
    let response = request(
        &app,
        "DELETE",
        &format!("{base}/%5F%5Fconsumer_offsets"),
        Some(json!({"confirmationName":"__consumer_offsets"})),
    )
    .await;
    assert_eq!(response.status(), 403);
    assert_eq!(
        response_json(response).await["code"],
        "INTERNAL_TOPIC_PROTECTED"
    );
    let recorded = calls.lock().await;
    assert!(recorded.deleted.is_empty());
    assert!(recorded.described.is_empty());
    assert!(recorded.updated.is_empty());
}

// All Admin variants have one stable, safe HTTP translation regardless of the operation.
#[tokio::test]
async fn native_errors_are_mapped_consistently_on_all_topic_routes() {
    for (error, status, code) in [
        (TopicAdminError::AlreadyExists, 409, "TOPIC_ALREADY_EXISTS"),
        (TopicAdminError::ConfigChanged, 409, "TOPIC_CONFIG_CHANGED"),
        (
            TopicAdminError::Authorization,
            403,
            "TOPIC_AUTHORIZATION_FAILED",
        ),
        (TopicAdminError::NotFound, 404, "TOPIC_NOT_FOUND"),
        (TopicAdminError::Timeout, 504, "TOPIC_OPERATION_TIMEOUT"),
        (TopicAdminError::InvalidConfig, 422, "INVALID_TOPIC_CONFIG"),
        (TopicAdminError::Unavailable, 502, "KAFKA_UNAVAILABLE"),
    ] {
        let (app, id, _) = app(Factory {
            error: Some(error),
            ..Factory::default()
        })
        .await;
        let base = format!("/api/sessions/{id}/topics");
        for (method, path, payload) in [
            ("POST", base.clone(), Some(json!({"name":"orders"}))),
            ("GET", format!("{base}/orders/configs"), None),
            (
                "PUT",
                format!("{base}/orders/configs"),
                Some(update_input()),
            ),
            (
                "DELETE",
                format!("{base}/orders"),
                Some(json!({"confirmationName":"orders"})),
            ),
        ] {
            let response = request(&app, method, &path, payload).await;
            assert_eq!(response.status(), status, "{method} {code}");
            let body = response_json(response).await;
            assert_eq!(body["code"], code);
            assert!(body["requestId"].is_string());
            assert!(body["details"].is_null());
        }
    }
}

// Closed and unknown IDs cannot regain access to a session's AdminClient through any route.
#[tokio::test]
async fn expired_sessions_return_gone_on_every_topic_route() {
    let (app, id, sessions) = app(Factory::default()).await;
    sessions.close(&id).await.unwrap();
    for session_id in [id.as_str(), "missing"] {
        let base = format!("/api/sessions/{session_id}/topics");
        for (method, path, payload) in [
            ("POST", base.clone(), Some(json!({"name":"orders"}))),
            ("GET", format!("{base}/orders/configs"), None),
            (
                "PUT",
                format!("{base}/orders/configs"),
                Some(update_input()),
            ),
            (
                "DELETE",
                format!("{base}/orders"),
                Some(json!({"confirmationName":"orders"})),
            ),
        ] {
            let response = request(&app, method, &path, payload).await;
            assert_eq!(response.status(), 410);
            assert_eq!(response_json(response).await["code"], "SESSION_EXPIRED");
        }
    }
}

// JSON rejections must use the common safe body and retain the global size boundary.
#[tokio::test]
async fn malformed_unknown_and_oversized_requests_have_safe_errors() {
    let factory = Factory::default();
    let calls = factory.calls.clone();
    let (app, id, _) = app(factory).await;
    let base = format!("/api/sessions/{id}/topics");
    for (method, path, body, status) in [
        (
            "POST",
            base.clone(),
            r#"{"name":false,"configs":{"password":"never-return-body"}}"#.into(),
            422,
        ),
        (
            "POST",
            base.clone(),
            r#"{"name":"orders","secret":"never-return-body"}"#.into(),
            422,
        ),
        (
            "PUT",
            format!("{base}/orders/configs"),
            "{never-return-body".into(),
            422,
        ),
        (
            "DELETE",
            format!("{base}/orders"),
            r#"{"confirmationName":"orders","secret":"never-return-body"}"#.into(),
            422,
        ),
        ("DELETE", format!("{base}/orders"), String::new(), 422),
        (
            "POST",
            base,
            format!(
                r#"{{"name":"orders","padding":"{}"}}"#,
                "x".repeat(256 * 1024)
            ),
            413,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let body = response_json(response).await;
        assert_eq!(
            body["code"],
            if status == 413 {
                "PAYLOAD_TOO_LARGE"
            } else {
                "VALIDATION_ERROR"
            }
        );
        assert!(!body.to_string().contains("never-return-body"));
        assert!(body["requestId"].is_string());
    }
    let recorded = calls.lock().await;
    assert!(recorded.created.is_empty());
    assert!(recorded.updated.is_empty());
    assert!(recorded.deleted.is_empty());
}
