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
    async fn send_text(&self, access_token: &str, text: &str) -> Result<(), AlertSendError>;
}

/// Validate before constructing a URL; callers may persist the normalized return value.
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReqwestFeishuSender([REDACTED])")
    }
}

impl ReqwestFeishuSender {
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
    async fn send_text(&self, access_token: &str, text: &str) -> Result<(), AlertSendError> {
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
    async fn execute(&self, request: reqwest::Request)
    -> Result<TransportResponse, AlertSendError>;
}

struct ReqwestTransport {
    client: reqwest::Client,
}

fn classify_reqwest_error(error: reqwest::Error) -> AlertSendError {
    if error.is_timeout() {
        AlertSendError::Timeout
    } else {
        AlertSendError::Network
    }
}

#[async_trait]
impl FeishuTransport for ReqwestTransport {
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
    fn assert_sensitive(condition: bool) {
        assert!(condition, "Feishu sender assertion failed");
    }

    #[test]
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
        async fn execute(&self, _: reqwest::Request) -> Result<TransportResponse, AlertSendError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
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
    fn test_message_clearly_identifies_test_cluster_and_time() {
        let detected_at =
            chrono::DateTime::parse_from_rfc3339("2026-10-22T09:30:00+08:00").unwrap();
        let text = format_test_alert("订单生产集群", detected_at);
        assert!(text.contains("测试通知"));
        assert!(text.contains("订单生产集群"));
        assert!(text.contains("2026-10-22T09:30:00+08:00"));
    }
}
