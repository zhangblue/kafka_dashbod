use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    // 在构建 Kafka 参数或持久化集群配置时，返回协议对应的稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInput {
    pub name: String,
    pub bootstrap_servers: String,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ca_pem: Option<String>,
    pub client_certificate_pem: Option<String>,
    pub client_key_pem: Option<String>,
    pub client_key_passphrase: Option<String>,
    #[serde(default = "default_timeout")]
    pub connection_timeout_ms: i64,
    #[serde(default = "default_normal")]
    pub default_normal_lag_max: i64,
    #[serde(default = "default_high")]
    pub default_high_lag_min: i64,
    #[serde(default = "default_duration")]
    pub default_over_limit_seconds: i64,
}

// 在反序列化缺省字段或构造明文输入时，提供连接超时毫秒数。
fn default_timeout() -> i64 {
    10_000
}
// 在反序列化缺省字段或构造明文输入时，提供默认正常 Lag 上限。
fn default_normal() -> i64 {
    999
}
// 在反序列化缺省字段或构造明文输入时，提供默认高 Lag 起始值。
fn default_high() -> i64 {
    5000
}
// 在反序列化缺省字段或构造明文输入时，提供持续超限秒数。
fn default_duration() -> i64 {
    30
}

impl ClusterInput {
    // 在测试或简化创建流程时构造无认证集群输入，同时填充默认阈值和超时。
    pub fn plaintext(name: impl Into<String>, bootstrap_servers: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bootstrap_servers: bootstrap_servers.into(),
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            username: None,
            password: None,
            ca_pem: None,
            client_certificate_pem: None,
            client_key_pem: None,
            client_key_passphrase: None,
            connection_timeout_ms: default_timeout(),
            default_normal_lag_max: default_normal(),
            default_high_lag_min: default_high(),
            default_over_limit_seconds: default_duration(),
        }
    }
}

impl std::fmt::Debug for ClusterInput {
    // 在调试格式化集群输入时输出固定脱敏文本，防止凭据随调试信息泄漏。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterInput([REDACTED])")
    }
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SecretClears {
    pub clear_password: bool,
    pub clear_ca_pem: bool,
    pub clear_client_certificate_pem: bool,
    pub clear_client_key_pem: bool,
    pub clear_client_key_passphrase: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterDto {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: String,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password_configured: bool,
    pub ca_pem_configured: bool,
    pub client_certificate_pem_configured: bool,
    pub client_key_pem_configured: bool,
    pub client_key_passphrase_configured: bool,
    pub connection_timeout_ms: i64,
    pub default_normal_lag_max: i64,
    pub default_high_lag_min: i64,
    pub default_over_limit_seconds: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone)]
pub struct DecryptedClusterConfig {
    pub bootstrap_servers: String,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ca_pem: Option<String>,
    pub client_certificate_pem: Option<String>,
    pub client_key_pem: Option<String>,
    pub client_key_passphrase: Option<String>,
    pub connection_timeout: Duration,
}

impl DecryptedClusterConfig {
    // 在测试或无需认证的连接场景中构造明文 Kafka 客户端配置。
    pub fn plaintext(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            username: None,
            password: None,
            ca_pem: None,
            client_certificate_pem: None,
            client_key_pem: None,
            client_key_passphrase: None,
            connection_timeout: Duration::from_secs(10),
        }
    }
}

impl std::fmt::Debug for DecryptedClusterConfig {
    // 在调试格式化内部连接配置时隐藏全部字段，防止密码和 PEM 泄漏。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DecryptedClusterConfig([REDACTED])")
    }
}
