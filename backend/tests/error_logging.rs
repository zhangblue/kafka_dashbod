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
    // 在日志捕获阶段把写入追加到共享内存，供脱敏断言检查完整日志。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(bytes)
    }
    // 满足测试内存日志写入器接口；写入已即时保存，无额外刷新工作。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct NativeErrorFactory;

// 构造带敏感上下文的原生失败，检查 API 和日志映射是否严格脱敏。
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

    // 为日志脱敏测试返回空 fake 客户端，使请求仅经过错误映射路径。
    async fn create(&self, _: DecryptedClusterConfig) -> Result<(), SessionError> {
        Ok(())
    }

    // 在创建 Topic 阶段注入含敏感上下文的原生失败，检查日志过滤。
    async fn create_topic(&self, _: &mut (), _: &CreateTopicInput) -> Result<(), TopicAdminError> {
        Err(native_failure())
    }

    // 在读取配置阶段注入原生失败，检查对应操作上下文与脱敏。
    async fn describe_topic_configs(
        &self,
        _: &mut (),
        _: &str,
    ) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
        Err(native_failure())
    }

    // 在更新配置阶段注入原生失败，防止修改路径绕过安全映射。
    async fn update_topic_configs(
        &self,
        _: &mut (),
        _: &str,
        _: &UpdateTopicConfigsInput,
    ) -> Result<TopicConfigUpdateOutcome, TopicAdminError> {
        Err(native_failure())
    }

    // 在删除阶段注入原生失败，检查高风险操作同样只输出安全诊断。
    async fn delete_topic(&self, _: &mut (), _: &str) -> Result<(), TopicAdminError> {
        Err(native_failure())
    }
}

// Administration errors must correlate to the cluster without recording native text or payloads.
// 验证所有 Topic 失败只记录稳定请求上下文，避免原生错误及凭据进入日志。
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
    // 先安装内存日志收集器再执行失败请求，才能同时检查响应和关联日志的脱敏边界。
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    // 在失败请求执行前绑定当前测试日志收集器，保留 guard 直到日志断言结束。
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
        // 每条路由前清空上一条日志，避免错误地用其他请求的关联上下文通过断言。
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
// 验证失败日志保留请求、集群和操作标识，但不包含秘密或查询字符串。
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
    // 在失败请求执行前绑定当前测试日志收集器，保留 guard 直到日志断言结束。
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
