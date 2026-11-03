use super::RepositoryError;
use crate::domain::lag_rule::{LagRule, LagRuleInput};
use sqlx::SqlitePool;
use std::collections::HashSet;

#[derive(Clone)]
pub struct LagRuleRepository {
    pool: SqlitePool,
}

impl LagRuleRepository {
    // 在组装 Lag 规则 API 依赖时从共享连接池创建仓储。
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // 在需要独立读取集群自定义规则时按 Group 和 Topic 排序返回覆盖规则。
    pub async fn list(&self, cluster_id: &str) -> Result<Vec<LagRule>, RepositoryError> {
        Ok(sqlx::query_as("SELECT id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds FROM lag_rules WHERE cluster_id = ? ORDER BY group_id, topic").bind(cluster_id).fetch_all(&self.pool).await?)
    }

    // 在仅替换自定义规则的调用中委托事务流程，保留集群默认阈值。
    pub async fn replace(
        &self,
        cluster_id: &str,
        rules: &[LagRuleInput],
    ) -> Result<Vec<LagRule>, RepositoryError> {
        self.replace_transaction(cluster_id, None, rules).await
    }

    /// Read default thresholds and overrides from a single consistent transaction.
    // 在 Lag 设置 API 读取时用同一事务获取默认阈值和覆盖规则，避免跨版本混读。
    pub async fn settings(
        &self,
        cluster_id: &str,
    ) -> Result<((i64, i64, i64), Vec<LagRule>), RepositoryError> {
        // 默认阈值与覆盖规则放在同一读事务中，避免并发保存时读取到跨版本组合。
        let mut tx = self.pool.begin().await?;
        let defaults = sqlx::query_as("SELECT default_normal_lag_max, default_high_lag_min, default_over_limit_seconds FROM clusters WHERE id = ?")
            .bind(cluster_id).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::ClusterNotFound)?;
        let rules = sqlx::query_as("SELECT id, cluster_id, group_id, topic, normal_lag_max, high_lag_min, over_limit_seconds FROM lag_rules WHERE cluster_id = ? ORDER BY group_id, topic")
            .bind(cluster_id).fetch_all(&mut *tx).await?;
        // 两类设置全部读取成功后结束事务，再向 API 返回同一版本的数据。
        tx.commit().await?;
        Ok((defaults, rules))
    }

    /// Atomically update cluster defaults and fully replace all overrides.
    // 在 Lag 设置 API 保存时先校验默认阈值，再原子更新默认值与全部覆盖规则。
    pub async fn replace_settings(
        &self,
        cluster_id: &str,
        defaults: (i64, i64, i64),
        rules: &[LagRuleInput],
    ) -> Result<Vec<LagRule>, RepositoryError> {
        // 在进入替换事务前拒绝无效默认区间，避免将非法阈值与覆盖规则一起保存。
        if defaults.0 < 0 || defaults.1 <= defaults.0 || defaults.2 < 0 {
            return Err(RepositoryError::InvalidInput);
        }
        self.replace_transaction(cluster_id, Some(defaults), rules)
            .await
    }

    // 在规则替换入口调用时校验整批规则并事务性删除重建，任一步失败回滚全部改动。
    async fn replace_transaction(
        &self,
        cluster_id: &str,
        defaults: Option<(i64, i64, i64)>,
        rules: &[LagRuleInput],
    ) -> Result<Vec<LagRule>, RepositoryError> {
        // 默认阈值更新与覆盖规则删除重建共享事务，任一步失败都不能提交部分设置。
        let mut tx = self.pool.begin().await?;
        // 先完整校验值域及 Group/Topic 唯一性，再执行删除重建，防止半批非法输入影响旧规则。
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
        // 默认值更新和旧规则删除、新规则插入必须在同一事务内；插入失败时恢复整套旧设置。
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
        // 所有新规则插入成功后才提交，保证默认值更新和整批覆盖规则一起生效。
        tx.commit().await?;
        Ok(saved)
    }
}
