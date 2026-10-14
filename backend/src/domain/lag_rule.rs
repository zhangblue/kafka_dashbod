use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LagRuleInput {
    pub group_id: String,
    pub topic: String,
    pub normal_lag_max: i64,
    pub high_lag_min: i64,
    pub over_limit_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct LagRule {
    pub id: String,
    pub cluster_id: String,
    pub group_id: String,
    pub topic: String,
    pub normal_lag_max: i64,
    pub high_lag_min: i64,
    pub over_limit_seconds: i64,
}
