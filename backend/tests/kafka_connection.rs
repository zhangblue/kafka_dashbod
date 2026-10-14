mod support;

use std::time::Duration;

use kafka_dashboard::{domain::cluster::DecryptedClusterConfig, kafka::client::test_connection};
use serde_json::json;
use support::*;

// The endpoint must load saved credentials, reject unsupported security before networking,
// and preserve the shared error envelope without echoing security settings.
#[tokio::test]
async fn connection_endpoint_rejects_invalid_security_and_unknown_clusters() {
    let ctx = test_app().await;
    let missing = request(&ctx.app, "POST", "/api/clusters/missing/test", None).await;
    assert_eq!(missing.status(), 404);
    assert_eq!(response_json(missing).await["code"], "CLUSTER_NOT_FOUND");

    let mut input = cluster("invalid-security");
    input["securityProtocol"] = json!("SASL_SSL");
    input["saslMechanism"] = json!("PLAIN");
    input["username"] = json!("reader");
    input["password"] = json!("test-password");
    let created = request(&ctx.app, "POST", "/api/clusters", Some(input)).await;
    assert_eq!(created.status(), 201);
    let id = response_json(created).await["id"]
        .as_str()
        .unwrap()
        .to_owned();
    // Older stored settings may predate validation at save time. The connection
    // boundary must still reject unsupported mechanisms without exposing values.
    sqlx::query("UPDATE clusters SET sasl_mechanism = 'GSSAPI' WHERE id = ?")
        .bind(&id)
        .execute(&ctx.pool)
        .await
        .unwrap();
    let response = request(&ctx.app, "POST", &format!("/api/clusters/{id}/test"), None).await;
    assert_eq!(response.status(), 422);
    let text = response_text(response).await;
    assert!(!text.contains("test-password"));
    assert!(!text.contains("GSSAPI"));
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["requestId"].is_string());
}

#[tokio::test]
async fn unavailable_broker_returns_bounded_safe_timeout() {
    let mut config = DecryptedClusterConfig::plaintext("127.0.0.1:1");
    config.connection_timeout = Duration::from_millis(200);
    let started = std::time::Instant::now();
    let error = test_connection(config).await.unwrap_err();
    assert_eq!(error.code(), "KAFKA_TIMEOUT");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn dns_errors_are_not_reported_as_timeouts() {
    // A label longer than the DNS limit fails local getaddrinfo validation.
    // A merely absent `.invalid` name may resolve under a DNS wildcard/proxy.
    let mut config = DecryptedClusterConfig::plaintext(format!("{}.invalid:9092", "a".repeat(64)));
    config.connection_timeout = Duration::from_millis(300);
    let error = test_connection(config).await.unwrap_err();
    assert_eq!(error.code(), "KAFKA_DNS_FAILED");
}

#[tokio::test]
async fn malformed_tls_material_has_a_safe_tls_error() {
    let mut config = DecryptedClusterConfig::plaintext("127.0.0.1:1");
    config.security_protocol = kafka_dashboard::domain::cluster::SecurityProtocol::Ssl;
    config.ca_pem = Some("test-secret-ca".into());
    config.connection_timeout = Duration::from_millis(200);
    let error = test_connection(config).await.unwrap_err();
    assert_eq!(error.code(), "KAFKA_TLS_FAILED");
    assert!(!format!("{error:?} {error}").contains("test-secret-ca"));
}

// Opt-in because it requires a running broker. CI starts docker-compose.test.yml first.
#[tokio::test]
#[ignore = "requires TEST_KAFKA_BROKERS and docker-compose.test.yml"]
async fn plaintext_broker_connection_succeeds() {
    let brokers = std::env::var("TEST_KAFKA_BROKERS")
        .expect("set TEST_KAFKA_BROKERS before running ignored Kafka integration tests");
    let result = test_connection(DecryptedClusterConfig::plaintext(brokers.clone()))
        .await
        .unwrap();
    assert!(result.broker_count >= 1);
    assert!(result.elapsed_ms < 12_000);

    let ctx = test_app().await;
    let mut input = cluster("connected");
    input["bootstrapServers"] = json!(brokers);
    let created = request(&ctx.app, "POST", "/api/clusters", Some(input)).await;
    let id = response_json(created).await["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = request(&ctx.app, "POST", &format!("/api/clusters/{id}/test"), None).await;
    assert_eq!(response.status(), 200);
    let body = response_json(response).await;
    assert!(body["brokerCount"].as_u64().unwrap() >= 1);
    assert!(body["topicCount"].is_number());
    assert!(body["elapsedMs"].is_number());
}
