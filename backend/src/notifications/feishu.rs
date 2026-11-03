use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc, time::Duration};

const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RESPONSE_BYTES: usize = 16 * 1024;
pub const MAX_ACCESS_TOKEN_BYTES: usize = 512;

/// Contains only stable, safe classifications, never an underlying HTTP error or response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AlertSendError {
    #[error("飞书 Access Token 无效")]
    InvalidToken,
    #[error("飞书通知客户端初始化失败")]
    ClientConfiguration,
    #[error("飞书通知发送超时")]
    Timeout,
    #[error("飞书通知网络连接失败")]
    Network,
    #[error("飞书通知服务返回非成功状态")]
    HttpStatus,
    #[error("飞书通知服务拒绝了消息")]
    Rejected,
    #[error("飞书通知服务响应无效")]
    InvalidResponse,
}

#[async_trait]
pub trait AlertSender: Send + Sync {
    // 通知触发时发送文本并返回稳定安全结果，供告警状态机记录本次投递。
    async fn send_text(&self, access_token: &str, text: &str) -> Result<(), AlertSendError>;
}

/// Validate before constructing a URL; callers may persist the normalized return value.
// 保存或发送前校验并规范化机器人凭据，拒绝无效路径片段及控制字符。
pub fn validate_access_token(access_token: &str) -> Result<&str, AlertSendError> {
    let token = access_token.trim();
    if token.is_empty()
        || token.len() > MAX_ACCESS_TOKEN_BYTES
        || access_token.chars().any(char::is_control)
        || matches!(token, "." | "..")
    {
        return Err(AlertSendError::InvalidToken);
    }
    Ok(token)
}

// 创建通知客户端时设置总时限、仅允许 HTTPS 并禁用重定向，约束凭据发送边界。
fn configured_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(SEND_TIMEOUT)
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
}

pub struct ReqwestFeishuSender {
    client: reqwest::Client,
    transport: Arc<dyn FeishuTransport>,
}

impl fmt::Debug for ReqwestFeishuSender {
    // 调试格式化时仅输出固定脱敏标记，避免检查内部连接配置。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReqwestFeishuSender([REDACTED])")
    }
}

impl ReqwestFeishuSender {
    // 应用初始化通知服务时创建受限客户端和生产传输适配器。
    pub fn new() -> Result<Self, AlertSendError> {
        let client = configured_client_builder()
            .build()
            .map_err(|_| AlertSendError::ClientConfiguration)?;
        Ok(Self {
            transport: Arc::new(ReqwestTransport {
                client: client.clone(),
            }),
            client,
        })
    }

    #[cfg(test)]
    // 通知单元测试创建发送器时注入假传输，避免访问真实通知服务。
    fn with_transport(transport: impl FeishuTransport + 'static) -> Self {
        Self {
            client: configured_client_builder().build().unwrap(),
            transport: Arc::new(transport),
        }
    }
}

#[derive(Serialize)]
struct TextPayload<'a> {
    msg_type: &'static str,
    content: TextContent<'a>,
}

#[derive(Serialize)]
struct TextContent<'a> {
    text: &'a str,
}

#[derive(Deserialize)]
struct BusinessResponse {
    code: i64,
}

#[async_trait]
impl AlertSender for ReqwestFeishuSender {
    // 通知触发时发送文本并返回稳定安全结果，供告警状态机记录本次投递。
    async fn send_text(&self, access_token: &str, text: &str) -> Result<(), AlertSendError> {
        // 先校验凭据，再作为单一路径片段编码；目标主机和机器人路径前缀保持固定。
        let token = validate_access_token(access_token)?;
        let mut url = reqwest::Url::parse("https://open.feishu.cn/open-apis/bot/v2/hook/")
            .map_err(|_| AlertSendError::ClientConfiguration)?;
        url.path_segments_mut()
            .map_err(|_| AlertSendError::ClientConfiguration)?
            .pop_if_empty()
            .push(token);
        let request = self
            .client
            .post(url)
            .timeout(SEND_TIMEOUT)
            .json(&TextPayload {
                msg_type: "text",
                content: TextContent { text },
            })
            .build()
            .map_err(|_| AlertSendError::ClientConfiguration)?;
        // 外层超时覆盖整个传输及响应体读取，假传输和生产传输都受相同五秒预算约束。
        let response = tokio::time::timeout(SEND_TIMEOUT, self.transport.execute(request))
            .await
            .map_err(|_| AlertSendError::Timeout)??;
        if !(200..300).contains(&response.status) {
            return Err(AlertSendError::HttpStatus);
        }
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err(AlertSendError::InvalidResponse);
        }
        let response: BusinessResponse =
            serde_json::from_slice(&response.body).map_err(|_| AlertSendError::InvalidResponse)?;
        if response.code != 0 {
            return Err(AlertSendError::Rejected);
        }
        Ok(())
    }
}

struct TransportResponse {
    status: u16,
    body: Vec<u8>,
}

#[async_trait]
trait FeishuTransport: Send + Sync {
    // 发送通知时执行请求并有界读取响应，非成功状态不读取远端正文。
    async fn execute(&self, request: reqwest::Request)
    -> Result<TransportResponse, AlertSendError>;
}

struct ReqwestTransport {
    client: reqwest::Client,
}

// 通知网络调用失败时只区分超时与网络故障，不暴露请求或响应内容。
fn classify_reqwest_error(error: reqwest::Error) -> AlertSendError {
    if error.is_timeout() {
        AlertSendError::Timeout
    } else {
        AlertSendError::Network
    }
}

#[async_trait]
impl FeishuTransport for ReqwestTransport {
    // 发送通知时执行请求并有界读取响应，非成功状态不读取远端正文。
    async fn execute(
        &self,
        request: reqwest::Request,
    ) -> Result<TransportResponse, AlertSendError> {
        let mut response = self
            .client
            .execute(request)
            .await
            .map_err(classify_reqwest_error)?;
        let status = response.status().as_u16();
        // 先检查状态再读取正文，失败响应不保留远端内容；成功体也必须受字节上限约束。
        // Reject unsuccessful status before reading any potentially sensitive remote body.
        if !response.status().is_success() {
            return Ok(TransportResponse {
                status,
                body: Vec::new(),
            });
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE_BYTES as u64)
        {
            return Err(AlertSendError::InvalidResponse);
        }
        let mut body = Vec::new();
        // 流式逐块累计上限，不依赖可能缺失或不可信的长度头，避免无限响应占用内存。
        while let Some(chunk) = response.chunk().await.map_err(classify_reqwest_error)? {
            if chunk.len() > MAX_RESPONSE_BYTES - body.len() {
                return Err(AlertSendError::InvalidResponse);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(TransportResponse { status, body })
    }
}

pub struct GrowthAlertMessage<'a> {
    pub cluster_name: &'a str,
    pub topic: &'a str,
    pub group_id: &'a str,
    pub threshold: u64,
    pub total_lag: u64,
    pub growth_seconds: u64,
    pub detected_at: DateTime<FixedOffset>,
}

// 拼装通知时为非负数量添加千分位，保持大整数完整精度。
fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

// 增长规则决定发送时生成含阈值、总 Lag、持续时间和时区的通知文本。
pub fn format_growth_alert(message: &GrowthAlertMessage<'_>) -> String {
    format!(
        "Kafka Lens Lag 持续增长告警\n集群：{}\nTopic：{}\nConsumer Group：{}\n阈值：{}\n当前积压量（全部 Partition Lag 总和）：{}\n连续增长时间：{} 秒\n检测时间：{}",
        message.cluster_name,
        message.topic,
        message.group_id,
        format_count(message.threshold),
        format_count(message.total_lag),
        message.growth_seconds,
        message.detected_at.to_rfc3339()
    )
}

// 生成明确标识测试用途的集群与时间文本；当前仅由本模块单元测试直接调用以验证文本格式。
pub fn format_test_alert(cluster_name: &str, detected_at: DateTime<FixedOffset>) -> String {
    format!(
        "Kafka Lens 飞书测试通知\n集群：{}\n检测时间：{}",
        cluster_name,
        detected_at.to_rfc3339()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[track_caller]
    // 敏感通知测试断言失败时仅输出固定诊断，避免泄漏请求上下文。
    fn assert_sensitive(condition: bool) {
        assert!(condition, "Feishu sender assertion failed");
    }

    #[test]
    // 验证敏感断言的失败诊断保持固定文本。
    fn sensitive_assertions_use_fixed_failure_diagnostics() {
        let token = String::from("diagnostic-fixture-token");
        let panic = std::panic::catch_unwind(|| assert_sensitive(token.is_empty()))
            .expect_err("sensitive assertion unexpectedly passed");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied());
        assert!(
            message == Some("Feishu sender assertion failed"),
            "sensitive assertion failure diagnostics were unsafe"
        );
    }

    #[derive(Clone)]
    struct RecordingTransport {
        requests: Arc<Mutex<Vec<reqwest::Request>>>,
        status: u16,
        body: Vec<u8>,
    }

    impl RecordingTransport {
        // 通知测试准备假传输时设置可控状态与响应体，并初始化请求记录。
        fn responding(status: u16, body: &[u8]) -> Self {
            Self {
                requests: Arc::default(),
                status,
                body: body.to_vec(),
            }
        }
    }

    #[async_trait]
    impl FeishuTransport for RecordingTransport {
        // 通知测试调用时返回可控传输结果，验证发送器行为。
        async fn execute(
            &self,
            request: reqwest::Request,
        ) -> Result<TransportResponse, AlertSendError> {
            self.requests.lock().unwrap().push(request);
            Ok(TransportResponse {
                status: self.status,
                body: self.body.clone(),
            })
        }
    }

    // Catch URL interpolation, missing JSON envelope, and accepting a response without code=0.
    #[tokio::test]
    // 验证通知正文符合文本消息结构且仅业务成功码被接受。
    async fn text_payload_and_success_code_follow_feishu_contract() {
        let transport = RecordingTransport::responding(200, br#"{"code":0,"msg":"success"}"#);
        let sender = ReqwestFeishuSender::with_transport(transport.clone());
        sender
            .send_text(" token/value?#% ", "Kafka test")
            .await
            .unwrap();
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.url().scheme(), "https");
        assert_eq!(request.url().host_str(), Some("open.feishu.cn"));
        assert_sensitive(request.url().path() == "/open-apis/bot/v2/hook/token%2Fvalue%3F%23%25");
        assert!(request.url().query().is_none());
        assert!(request.url().fragment().is_none());
        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(request.timeout(), Some(&Duration::from_secs(5)));
        assert_eq!(
            request.headers()[reqwest::header::CONTENT_TYPE],
            "application/json"
        );
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_sensitive(body == json!({"msg_type":"text","content":{"text":"Kafka test"}}));
    }

    #[tokio::test]
    // 验证机器人凭据不能改变目标主机或越出固定路径前缀。
    async fn token_cannot_change_host_or_escape_the_hook_prefix() {
        for token in [
            "https://evil.invalid/hook",
            "../escape",
            "..",
            ".",
            "%2f",
            "//evil.invalid",
        ] {
            let transport = RecordingTransport::responding(200, br#"{"code":0}"#);
            let sender = ReqwestFeishuSender::with_transport(transport.clone());
            let result = sender.send_text(token, "test").await;
            if token == "." || token == ".." {
                assert_eq!(result, Err(AlertSendError::InvalidToken));
            } else {
                result.unwrap();
                let requests = transport.requests.lock().unwrap();
                let url = requests[0].url();
                assert_eq!(url.host_str(), Some("open.feishu.cn"));
                assert!(url.path().starts_with("/open-apis/bot/v2/hook/"));
                assert_eq!(url.path_segments().unwrap().count(), 5);
            }
        }
    }

    #[tokio::test]
    // 验证无效机器人凭据在进入传输前被拒绝。
    async fn invalid_tokens_are_rejected_before_transport() {
        for token in [
            String::new(),
            "  ".to_owned(),
            "secret\n".to_owned(),
            "sec\0ret".to_owned(),
            "a".repeat(513),
            "界".repeat(171),
        ] {
            let transport = RecordingTransport::responding(200, br#"{"code":0}"#);
            let sender = ReqwestFeishuSender::with_transport(transport.clone());
            assert_eq!(
                sender.send_text(&token, "test").await,
                Err(AlertSendError::InvalidToken)
            );
            assert!(transport.requests.lock().unwrap().is_empty());
        }
        assert!(validate_access_token(&"a".repeat(512)).is_ok());
        assert!(validate_access_token(&"é".repeat(256)).is_ok());
    }

    #[tokio::test]
    // 验证 HTTP 与业务失败转换为安全分类。
    async fn http_and_business_failures_are_safely_classified() {
        for (status, body, expected) in [
            (302, br#"{"code":0}"#.as_slice(), AlertSendError::HttpStatus),
            (500, br#"{"code":0}"#.as_slice(), AlertSendError::HttpStatus),
            (
                200,
                br#"{"code":19024,"msg":"remote-secret"}"#.as_slice(),
                AlertSendError::Rejected,
            ),
            (
                200,
                br#"{"StatusCode":0}"#.as_slice(),
                AlertSendError::InvalidResponse,
            ),
            (
                200,
                br#"{"code":"0"}"#.as_slice(),
                AlertSendError::InvalidResponse,
            ),
            (
                200,
                b"remote-secret".as_slice(),
                AlertSendError::InvalidResponse,
            ),
        ] {
            let transport = RecordingTransport::responding(status, body);
            let sender = ReqwestFeishuSender::with_transport(transport);
            let error = sender
                .send_text("private-test-token", "test")
                .await
                .unwrap_err();
            assert_eq!(error, expected);
            let diagnostics = format!("{sender:?} {error:?} {error}");
            assert_sensitive(!diagnostics.contains("private-test-token"));
            assert_sensitive(!diagnostics.contains("remote-secret"));
            assert_sensitive(!diagnostics.contains("test\""));
        }
    }

    #[tokio::test]
    // 验证超大业务响应被拒绝，避免无限读取。
    async fn oversized_business_response_is_rejected() {
        let body = format!(r#"{{"code":0,"msg":"{}"}}"#, "a".repeat(16 * 1024));
        let sender = ReqwestFeishuSender::with_transport(RecordingTransport::responding(
            200,
            body.as_bytes(),
        ));
        assert_eq!(
            sender.send_text("token", "test").await,
            Err(AlertSendError::InvalidResponse)
        );
    }

    struct HangingTransport;

    #[async_trait]
    impl FeishuTransport for HangingTransport {
        // 超时测试调用时保持传输挂起，验证发送器外层总时限能够终止等待。
        async fn execute(&self, _: reqwest::Request) -> Result<TransportResponse, AlertSendError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    // 验证完整发送链路在五秒后超时。
    async fn total_send_times_out_after_five_seconds() {
        let sender = ReqwestFeishuSender::with_transport(HangingTransport);
        let started = tokio::time::Instant::now();
        assert_eq!(
            sender.send_text("private-test-token", "test").await,
            Err(AlertSendError::Timeout)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(5));
    }

    #[tokio::test]
    // 验证传输超时与网络失败保持不同分类。
    async fn transport_failures_keep_timeout_distinct_from_network_errors() {
        for should_timeout in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                if should_timeout {
                    std::future::pending::<()>().await;
                }
                drop(socket);
            });
            let client = configured_client_builder()
                .https_only(false)
                .no_proxy()
                .build()
                .unwrap();
            let request = client
                .get(format!("http://{address}/"))
                .timeout(Duration::from_millis(100))
                .build()
                .unwrap();
            let result = ReqwestTransport { client }.execute(request).await;
            let expected = if should_timeout {
                AlertSendError::Timeout
            } else {
                AlertSendError::Network
            };
            assert!(matches!(result, Err(error) if error == expected));
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    // 验证生产客户端拒绝明文 HTTP 请求。
    async fn production_client_rejects_plain_http() {
        let client = configured_client_builder().build().unwrap();
        assert!(
            client
                .get("http://127.0.0.1:1/")
                .send()
                .await
                .unwrap_err()
                .is_builder()
        );
    }

    // Allow only loopback HTTP in this test to exercise the production redirect policy.
    #[tokio::test]
    // 验证通知客户端不会跟随重定向转发敏感请求。
    async fn configured_client_does_not_follow_redirects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{address}/redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
        });
        let client = configured_client_builder()
            .https_only(false)
            .no_proxy()
            .build()
            .unwrap();
        let response = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 302);
        server.await.unwrap();
    }

    #[test]
    // 验证增长通知包含所有必要字段与明确时区。
    fn growth_message_has_all_required_fields_and_timezone() {
        let text = format_growth_alert(&GrowthAlertMessage {
            cluster_name: "订单生产集群",
            topic: "orders",
            group_id: "order-service",
            threshold: 5_000,
            total_lag: 12_580,
            growth_seconds: 60,
            detected_at: chrono::DateTime::parse_from_rfc3339("2026-10-22T09:30:00+08:00").unwrap(),
        });
        for required in [
            "订单生产集群",
            "orders",
            "order-service",
            "5,000",
            "当前积压量（全部 Partition Lag 总和）：12,580",
            "60",
            "2026-10-22T09:30:00+08:00",
        ] {
            assert!(text.contains(required));
        }
    }

    #[test]
    // 验证增长持续时间超过 32 位无符号整数范围时不会截断。
    fn growth_message_preserves_durations_above_u32_max() {
        let text = format_growth_alert(&GrowthAlertMessage {
            cluster_name: "fixture",
            topic: "t",
            group_id: "g",
            threshold: 5_000,
            total_lag: 6_000,
            growth_seconds: 4_294_967_296_u64,
            detected_at: chrono::DateTime::parse_from_rfc3339("2026-10-22T09:30:00+08:00").unwrap(),
        });
        assert!(text.contains("连续增长时间：4294967296 秒"));
    }

    #[test]
    // 验证测试通知明确包含测试用途、集群与时间。
    fn test_message_clearly_identifies_test_cluster_and_time() {
        let detected_at =
            chrono::DateTime::parse_from_rfc3339("2026-10-22T09:30:00+08:00").unwrap();
        let text = format_test_alert("订单生产集群", detected_at);
        assert!(text.contains("测试通知"));
        assert!(text.contains("订单生产集群"));
        assert!(text.contains("2026-10-22T09:30:00+08:00"));
    }
}
