use serde::{Deserialize, Serialize};

// Lag 设置保存请求中的 Group + Topic 覆盖项；持久化前由仓储统一校验整批阈值。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LagRuleInput {
    pub group_id: String,
    pub topic: String,
    pub normal_lag_max: i64,
    pub high_lag_min: i64,
    pub over_limit_seconds: i64,
}

// 仓储读取后返回的覆盖规则，用于页面以 Group + Topic 覆盖集群默认阈值。
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
