mod support;

use async_trait::async_trait;
use axum::{body::Body, http::Request};
use kafka_dashboard::{
    db::feishu_alerts::FeishuAlertRepository,
    notifications::feishu::{AlertSendError, AlertSender},
};
use serde_json::{Value, json};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};
use support::*;
use tower::ServiceExt;

const SAVED_TOKEN: &str = "fixture-saved-token-secret";
const DRAFT_TOKEN: &str = "fixture-draft-token-secret";

// Do not put values derived from a token, response, message, or log in panic diagnostics.
// 在告警 API 的敏感断言中使用固定错误文本，避免失败时打印 Token。
#[track_caller]
fn assert_safe(condition: bool) {
    assert!(condition, "Feishu API assertion failed");
}

#[derive(Clone, Default)]
struct FakeAlertSender {
    sent: Arc<Mutex<Vec<(String, String)>>>,
    error: Option<AlertSendError>,
}

#[async_trait]
impl AlertSender for FakeAlertSender {
    // 在测试通知阶段记录 Token 和消息并返回预设结果，隔离真实网络发送。
    async fn send_text(&self, token: &str, text: &str) -> Result<(), AlertSendError> {
        self.sent.lock().unwrap().push((token.into(), text.into()));
        self.error.map_or(Ok(()), Err)
    }
}

// 在告警 API 测试准备阶段构造完整有效的 Token 与规则请求。
fn settings() -> Value {
    json!({"accessToken": SAVED_TOKEN, "clearAccessToken":false,
        "rules":[{"groupId":"g","topic":"t","lagThreshold":5000,"growthSeconds":10,"enabled":true}]})
}

// 在告警响应断言阶段先检查正文不含敏感测试值，再解析 JSON。
async fn safe_json(response: axum::response::Response) -> Value {
    // 只用固定诊断检查敏感正文后再解析，避免 JSON 失败诊断回显 Token。
    let text = response_text(response).await;
    assert_safe(!text.contains(SAVED_TOKEN) && !text.contains(DRAFT_TOKEN));
    serde_json::from_str(&text).unwrap_or_else(|_| panic!("Expected safe JSON response"))
}

// Missing routing or exposing a stored token must fail this public settings round trip.
// 验证保存只返回掩码配置，测试通知正确使用已存 Token。
#[tokio::test]
async fn put_returns_masked_settings_and_test_uses_saved_token() {
    let sender = FakeAlertSender::default();
    let fixture = test_app_with_sender(sender.clone()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    let initial = request(&fixture.app, "GET", &uri, None).await;
    assert_eq!(initial.status(), 200);
    assert_safe(
        safe_json(initial).await
            == json!({"accessTokenConfigured":false,"maskedAccessToken":null,"rules":[]}),
    );
    let response = request(&fixture.app, "PUT", &uri, Some(settings())).await;
    assert_eq!(response.status(), 200);
    let saved = safe_json(response).await;
    assert_safe(saved["accessTokenConfigured"] == true);
    assert_safe(saved["maskedAccessToken"] == "••••••••cret");
    assert_safe(saved["accessToken"].is_null());
    assert_safe(saved["rules"][0]["id"].is_string());
    assert_safe(saved["rules"][0]["growthSeconds"] == 10);
    assert_safe(safe_json(request(&fixture.app, "GET", &uri, None).await).await == saved);
    let response = request(
        &fixture.app,
        "POST",
        &format!("{uri}/test"),
        Some(json!({})),
    )
    .await;
    assert_eq!(response.status(), 200);
    safe_json(response).await;
    let sent = sender.sent.lock().unwrap();
    assert_safe(sent.len() == 1);
    assert_safe(sent[0].0 == SAVED_TOKEN);
    assert_safe(sent[0].1 == "飞书通知测试\n集群：prod");
}

// Empty or omitted replacements preserve the token, while clear removes it and replaces rules.
// 验证空白或省略 Token 保留原值，显式清除才移除已存值。
#[tokio::test]
async fn blank_or_omitted_token_preserves_and_clear_removes_saved_token() {
    let fixture = test_app_with_sender(FakeAlertSender::default()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    safe_json(request(&fixture.app, "PUT", &uri, Some(settings())).await).await;
    let repository = FeishuAlertRepository::new(fixture.pool.clone());
    for token in [None, Some(""), Some("   ")] {
        let mut input = json!({"clearAccessToken":false,"rules":[]});
        if let Some(token) = token {
            input["accessToken"] = json!(token);
        }
        let response = request(&fixture.app, "PUT", &uri, Some(input)).await;
        assert_eq!(response.status(), 200);
        let body = safe_json(response).await;
        assert_safe(body["accessTokenConfigured"] == true && body["rules"] == json!([]));
        assert_safe(repository.token(&id).await.unwrap().as_deref() == Some(SAVED_TOKEN));
    }
    let response = request(
        &fixture.app,
        "PUT",
        &uri,
        Some(json!({"accessToken":" ","clearAccessToken":true,"rules":[]})),
    )
    .await;
    assert_eq!(response.status(), 200);
    let body = safe_json(response).await;
    assert_safe(body["accessTokenConfigured"] == false && body["maskedAccessToken"].is_null());
    assert_safe(repository.token(&id).await.unwrap().is_none());
    let response = request(
        &fixture.app,
        "POST",
        &format!("{uri}/test"),
        Some(json!({})),
    )
    .await;
    assert_eq!(response.status(), 422);
    assert_safe(safe_json(response).await["details"]["accessToken"].is_string());
}

// A draft test must use the unsaved token without changing the persisted token or rule list.
// 验证测试优先使用去空白后的草稿 Token，且支持未保存配置而不写入。
#[tokio::test]
async fn test_prefers_trimmed_draft_without_persisting_and_accepts_unsaved_settings() {
    let sender = FakeAlertSender::default();
    let fixture = test_app_with_sender(sender.clone()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    for saved in [false, true] {
        if saved {
            safe_json(request(&fixture.app, "PUT", &uri, Some(settings())).await).await;
        }
        let before = safe_json(request(&fixture.app, "GET", &uri, None).await).await;
        let response = request(
            &fixture.app,
            "POST",
            &format!("{uri}/test"),
            Some(json!({"accessToken":format!("  {DRAFT_TOKEN}  ")})),
        )
        .await;
        assert_eq!(response.status(), 200);
        safe_json(response).await;
        assert_safe(safe_json(request(&fixture.app, "GET", &uri, None).await).await == before);
    }
    let sent = sender.sent.lock().unwrap();
    assert_safe(sent.len() == 2 && sent.iter().all(|(token, _)| token == DRAFT_TOKEN));
}

// Validation must reject each invalid field before the repository changes token or rules.
// 验证非法字段与去空白后重复组合被原子拒绝，防止部分更新。
#[tokio::test]
async fn invalid_fields_and_trimmed_duplicate_pairs_are_rejected_atomically() {
    let fixture = test_app_with_sender(FakeAlertSender::default()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    safe_json(request(&fixture.app, "PUT", &uri, Some(settings())).await).await;
    let before = safe_json(request(&fixture.app, "GET", &uri, None).await).await;
    for (field, value) in [
        ("groupId", json!(" ")),
        ("topic", json!("\t")),
        ("lagThreshold", json!(-1)),
        ("growthSeconds", json!(9)),
    ] {
        let mut input = settings();
        input["accessToken"] = json!(DRAFT_TOKEN);
        input["rules"][0][field] = value;
        let response = request(&fixture.app, "PUT", &uri, Some(input)).await;
        assert_eq!(response.status(), 422);
        let body = safe_json(response).await;
        assert_safe(body["code"] == "VALIDATION_ERROR");
        assert_safe(body["details"][format!("rules.0.{field}")].is_string());
    }
    let mut input = settings();
    let mut duplicate = input["rules"][0].clone();
    duplicate["groupId"] = json!(" g ");
    duplicate["topic"] = json!(" t ");
    input["rules"].as_array_mut().unwrap().push(duplicate);
    let response = request(&fixture.app, "PUT", &uri, Some(input)).await;
    assert_eq!(response.status(), 422);
    assert_safe(safe_json(response).await["details"]["rules.1.groupId"].is_string());
    let mut input = settings();
    input["clearAccessToken"] = json!(true);
    let response = request(&fixture.app, "PUT", &uri, Some(input)).await;
    assert_eq!(response.status(), 422);
    assert_safe(safe_json(response).await["details"]["accessToken"].is_string());
    assert_safe(safe_json(request(&fixture.app, "GET", &uri, None).await).await == before);
    assert_safe(
        FeishuAlertRepository::new(fixture.pool)
            .token(&id)
            .await
            .unwrap()
            .as_deref()
            == Some(SAVED_TOKEN),
    );
}

// JSON numbers must round-trip exactly through JavaScript without partial saves on failure.
// 验证安全整数最大值可保存，超界后拒绝且保持原配置。
#[tokio::test]
async fn safe_integer_maximum_is_accepted_and_larger_values_are_rejected_atomically() {
    let fixture = test_app_with_sender(FakeAlertSender::default()).await;
    let id = create_cluster(&fixture.app, "bounds").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    let mut input = settings();
    input["rules"][0]["lagThreshold"] = json!(9_007_199_254_740_991_i64);
    input["rules"][0]["growthSeconds"] = json!(9_007_199_254_740_991_i64);
    let response = request(&fixture.app, "PUT", &uri, Some(input.clone())).await;
    assert_eq!(response.status(), 200);
    let saved = safe_json(response).await;
    assert_safe(saved["rules"][0]["lagThreshold"] == 9_007_199_254_740_991_i64);
    assert_safe(saved["rules"][0]["growthSeconds"] == 9_007_199_254_740_991_i64);
    let repository = FeishuAlertRepository::new(fixture.pool.clone());
    let revision = repository.enabled_clusters().await.unwrap()[0]
        .revision
        .clone();
    for field in ["lagThreshold", "growthSeconds"] {
        let mut invalid = input.clone();
        invalid["accessToken"] = json!(DRAFT_TOKEN);
        invalid["rules"][0][field] = json!(9_007_199_254_740_992_i64);
        let response = request(&fixture.app, "PUT", &uri, Some(invalid)).await;
        assert_eq!(response.status(), 422);
        let error = safe_json(response).await;
        assert_safe(error["code"] == "VALIDATION_ERROR");
        assert_safe(error["details"][format!("rules.0.{field}")].is_string());
        assert_safe(error["requestId"].is_string());
        assert_safe(safe_json(request(&fixture.app, "GET", &uri, None).await).await == saved);
        assert_safe(repository.token(&id).await.unwrap().as_deref() == Some(SAVED_TOKEN));
        assert_safe(repository.enabled_clusters().await.unwrap()[0].revision == revision);
    }
}

// Normalized strings are stored, zero and ten-second boundaries accepted, IDs survive edits.
// 验证保存规范化组合、零阈值及合法规则标识，避免重建时丢失身份。
#[tokio::test]
async fn saves_trimmed_pairs_zero_threshold_and_preserves_valid_rule_ids() {
    let fixture = test_app_with_sender(FakeAlertSender::default()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    let mut input = settings();
    input["accessToken"] = json!(format!(" {SAVED_TOKEN} "));
    input["rules"][0]["groupId"] = json!(" g ");
    input["rules"][0]["topic"] = json!(" t ");
    input["rules"][0]["lagThreshold"] = json!(0);
    let response = request(&fixture.app, "PUT", &uri, Some(input)).await;
    assert_eq!(response.status(), 200);
    let saved = safe_json(response).await;
    assert_safe(saved["rules"][0]["groupId"] == "g" && saved["rules"][0]["topic"] == "t");
    assert_safe(
        FeishuAlertRepository::new(fixture.pool)
            .token(&id)
            .await
            .unwrap()
            .as_deref()
            == Some(SAVED_TOKEN),
    );
    let response = request(
        &fixture.app,
        "PUT",
        &uri,
        Some(json!({"rules":saved["rules"]})),
    )
    .await;
    assert_eq!(response.status(), 200);
    assert_safe(safe_json(response).await == saved);
}

// Token validation must use bytes, reject control characters and never send invalid drafts.
// 验证保存及测试均安全拒绝非法 Token，字段错误不能回显输入。
#[tokio::test]
async fn invalid_tokens_are_safe_field_errors_on_save_and_test() {
    let sender = FakeAlertSender::default();
    let fixture = test_app_with_sender(sender.clone()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let uri = format!("/api/clusters/{id}/feishu-alert");
    for token in [
        "a".repeat(513),
        "中".repeat(171),
        format!("{DRAFT_TOKEN}\n"),
        format!("{DRAFT_TOKEN}\u{7f}"),
        ".".into(),
        "..".into(),
    ] {
        for (method, path) in [("PUT", uri.clone()), ("POST", format!("{uri}/test"))] {
            let mut input = settings();
            input["accessToken"] = json!(token);
            let response = request(&fixture.app, method, &path, Some(input)).await;
            assert_eq!(response.status(), 422);
            let body = safe_json(response).await;
            assert_safe(body["details"]["accessToken"].is_string());
        }
    }
    assert_safe(sender.sent.lock().unwrap().is_empty());
    let response = request(
        &fixture.app,
        "PUT",
        &uri,
        Some(json!({"accessToken":"a".repeat(512),"rules":[]})),
    )
    .await;
    assert_eq!(response.status(), 200);
    safe_json(response).await;
}

// All three endpoints must use a cluster 404, including a test supplied with a new token.
// 验证所有告警端点对缺失集群返回安全 404，避免隐式创建配置。
#[tokio::test]
async fn missing_cluster_returns_safe_404_on_all_endpoints() {
    let sender = FakeAlertSender::default();
    let fixture = test_app_with_sender(sender.clone()).await;
    for (method, path) in [
        ("GET", "/api/clusters/missing/feishu-alert"),
        ("PUT", "/api/clusters/missing/feishu-alert"),
        ("POST", "/api/clusters/missing/feishu-alert/test"),
    ] {
        let response = request(&fixture.app, method, path, Some(settings())).await;
        assert_eq!(response.status(), 404);
        assert_safe(safe_json(response).await["code"] == "CLUSTER_NOT_FOUND");
    }
    assert_safe(sender.sent.lock().unwrap().is_empty());
}

// Malformed JSON and oversized bodies on both write endpoints retain the global safe envelope.
// 验证 JSON 拒绝和正文超限不回显请求体，保护 Token 等输入。
#[tokio::test]
async fn rejected_json_and_body_limit_never_echo_request_body() {
    let fixture = test_app_with_sender(FakeAlertSender::default()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    for (method, path) in [
        ("PUT", format!("/api/clusters/{id}/feishu-alert")),
        ("POST", format!("/api/clusters/{id}/feishu-alert/test")),
    ] {
        for (token, expected) in [
            (json!({"private":SAVED_TOKEN}), 422),
            (json!("a".repeat(256 * 1024)), 413),
        ] {
            let response = request(
                &fixture.app,
                method,
                &path,
                Some(json!({"accessToken":token,"rules":SAVED_TOKEN})),
            )
            .await;
            assert_eq!(response.status(), expected);
            let body = safe_json(response).await;
            assert_safe(body["requestId"].is_string());
        }
    }
}

#[derive(Clone, Default)]
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

// Sending failures must collapse to one safe error with correlation, without logging payloads.
// 验证发送失败返回安全错误及请求标识，日志不得包含 Token 或原生内容。
#[tokio::test]
async fn failed_send_returns_safe_error_and_request_id_without_token_in_logs() {
    let sender = FakeAlertSender {
        error: Some(AlertSendError::Rejected),
        ..Default::default()
    };
    let fixture = test_app_with_sender(sender).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let logs = LogBuffer::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    // This executable installs one global subscriber so concurrent route callsites cannot
    // cache a disabled thread-local subscriber while this request is being polled.
    tracing::subscriber::set_global_default(subscriber).expect("Unable to capture safe API logs");
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/clusters/{id}/feishu-alert/test?private={SAVED_TOKEN}"
                ))
                .header("content-type", "application/json")
                .header("x-request-id", "feishu-api-test-request")
                .body(Body::from(json!({"accessToken":DRAFT_TOKEN}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    let body = safe_json(response).await;
    assert_safe(
        body["code"] == "FEISHU_SEND_FAILED" && body["requestId"] == "feishu-api-test-request",
    );
    // 只通过固定诊断检查捕获日志，失败时不能把可能含秘密的日志整体打印。
    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert_safe(
        output.contains("FEISHU_SEND_FAILED") && output.contains("feishu-api-test-request"),
    );
    assert_safe(!output.contains(SAVED_TOKEN) && !output.contains(DRAFT_TOKEN));
    assert_safe(!output.contains("accessToken") && !output.contains("private="));
}

// 验证敏感断言诊断固定，防止断言失败输出秘密。
#[test]
fn sensitive_assertion_diagnostics_are_fixed() {
    let panic =
        std::panic::catch_unwind(|| assert_safe(false)).expect_err("Expected assertion failure");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied());
    assert_safe(message == Some("Feishu API assertion failed"));
}
