use super::RepositoryError;
use crate::domain::lag_rule::{LagRule, LagRuleInput};
use sqlx::SqlitePool;
use std::collections::HashSet;

#[derive(Clone)]
pub struct LagRuleRepository {
    pool: SqlitePool,
}

impl LagRuleRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list(&self, cluster_id: &str) -> Result<Vec<LagRule>, RepositoryError> {
        Ok(sqlx::query_as("SELECT id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds FROM lag_rules WHERE cluster_id = ? ORDER BY group_id, topic").bind(cluster_id).fetch_all(&self.pool).await?)
    }

    pub async fn replace(
        &self,
        cluster_id: &str,
        rules: &[LagRuleInput],
    ) -> Result<Vec<LagRule>, RepositoryError> {
        self.replace_transaction(cluster_id, None, rules).await
    }

    /// Read default thresholds and overrides from a single consistent transaction.
    pub async fn settings(
        &self,
        cluster_id: &str,
    ) -> Result<((i64, i64, i64), Vec<LagRule>), RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let defaults = sqlx::query_as("SELECT default_normal_lag_max, default_high_lag_min, default_over_limit_seconds FROM clusters WHERE id = ?")
            .bind(cluster_id).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::ClusterNotFound)?;
        let rules = sqlx::query_as("SELECT id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds FROM lag_rules WHERE cluster_id = ? ORDER BY group_id, topic")
            .bind(cluster_id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok((defaults, rules))
    }

    /// Atomically update cluster defaults and fully replace all overrides.
    pub async fn replace_settings(
        &self,
        cluster_id: &str,
        defaults: (i64, i64, i64),
        rules: &[LagRuleInput],
    ) -> Result<Vec<LagRule>, RepositoryError> {
        if defaults.0 < 0 || defaults.1 <= defaults.0 || defaults.2 < 0 {
            return Err(RepositoryError::InvalidInput);
        }
        self.replace_transaction(cluster_id, Some(defaults), rules)
            .await
    }

    async fn replace_transaction(
        &self,
        cluster_id: &str,
        defaults: Option<(i64, i64, i64)>,
        rules: &[LagRuleInput],
    ) -> Result<Vec<LagRule>, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let mut unique = HashSet::new();
        for rule in rules {
            if rule.group_id.trim().is_empty()
                || rule.topic.trim().is_empty()
                || rule.normal_lag_max < 0
                || rule.high_lag_min <= rule.normal_lag_max
                || rule.over_limit_seconds < 0
                || !unique.insert((&rule.group_id, &rule.topic))
            {
                return Err(RepositoryError::InvalidInput);
            }
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clusters WHERE id = ?)")
            .bind(cluster_id)
            .fetch_one(&mut *tx)
            .await?;
        if !exists {
            return Err(RepositoryError::ClusterNotFound);
        }
        if let Some((normal, high, duration)) = defaults {
            sqlx::query("UPDATE clusters SET default_normal_lag_max = ?, default_high_lag_min = ?, default_over_limit_seconds = ?, updated_at = ? WHERE id = ?")
                .bind(normal).bind(high).bind(duration).bind(chrono::Utc::now().to_rfc3339()).bind(cluster_id).execute(&mut *tx).await?;
        }
        sqlx::query("DELETE FROM lag_rules WHERE cluster_id = ?")
            .bind(cluster_id)
            .execute(&mut *tx)
            .await?;
        let mut saved = Vec::with_capacity(rules.len());
        for rule in rules {
            let id = uuid::Uuid::new_v4().to_string();
            sqlx::query("INSERT INTO lag_rules (id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds) VALUES (?, ?, ?, ?, ?, ?, ?)").bind(&id).bind(cluster_id).bind(&rule.group_id).bind(&rule.topic).bind(rule.normal_lag_max).bind(rule.high_lag_min).bind(rule.over_limit_seconds).execute(&mut *tx).await?;
            saved.push(LagRule {
                id,
                cluster_id: cluster_id.into(),
                group_id: rule.group_id.clone(),
                topic: rule.topic.clone(),
                normal_lag_max: rule.normal_lag_max,
                high_lag_min: rule.high_lag_min,
                over_limit_seconds: rule.over_limit_seconds,
            });
        }
        tx.commit().await?;
        Ok(saved)
    }
}
