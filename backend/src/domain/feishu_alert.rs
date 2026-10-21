use serde::{Deserialize, Serialize};

pub const MIN_GROWTH_SECONDS: i64 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeishuAlertRuleInput {
    pub id: Option<String>,
    pub group_id: String,
    pub topic: String,
    pub lag_threshold: i64,
    pub growth_seconds: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct FeishuAlertRule {
    pub id: String,
    pub group_id: String,
    pub topic: String,
    pub lag_threshold: i64,
    pub growth_seconds: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeishuAlertSettings {
    pub access_token_configured: bool,
    pub masked_access_token: Option<String>,
    pub rules: Vec<FeishuAlertRule>,
}

/// Internal worker configuration; deliberately not serializable.
#[derive(Clone)]
pub struct EnabledAlertCluster {
    pub cluster_id: String,
    pub access_token: String,
    pub rules: Vec<FeishuAlertRule>,
    pub revision: String,
}

impl std::fmt::Debug for EnabledAlertCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EnabledAlertCluster([REDACTED])")
    }
}
