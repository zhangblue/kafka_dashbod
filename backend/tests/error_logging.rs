mod support;

use axum::{body::Body, http::Request};
use std::{
    io::Write,
    sync::{Arc, Mutex},
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
