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
    #[serde(default = "default_timeout")]
    pub connection_timeout_ms: i64,
    #[serde(default = "default_normal")]
    pub default_normal_lag_max: i64,
    #[serde(default = "default_high")]
    pub default_high_lag_min: i64,
    #[serde(default = "default_duration")]
    pub default_over_limit_seconds: i64,
}

fn default_timeout() -> i64 {
    10_000
}
fn default_normal() -> i64 {
    999
}
fn default_high() -> i64 {
    5000
}
fn default_duration() -> i64 {
    30
}

impl ClusterInput {
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
            connection_timeout_ms: default_timeout(),
            default_normal_lag_max: default_normal(),
            default_high_lag_min: default_high(),
            default_over_limit_seconds: default_duration(),
        }
    }
}

impl std::fmt::Debug for ClusterInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterInput([REDACTED])")
    }
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
    pub connection_timeout: Duration,
}

impl DecryptedClusterConfig {
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
            connection_timeout: Duration::from_secs(10),
        }
    }
}

impl std::fmt::Debug for DecryptedClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DecryptedClusterConfig([REDACTED])")
    }
}
