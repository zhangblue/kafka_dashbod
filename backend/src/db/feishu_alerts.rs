use super::RepositoryError;
use crate::domain::feishu_alert::{
    EnabledAlertCluster, FeishuAlertRule, FeishuAlertRuleInput, FeishuAlertSettings,
    MAX_SAFE_INTEGER, MIN_GROWTH_SECONDS,
};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct FeishuAlertRepository {
    pool: SqlitePool,
}

impl FeishuAlertRepository {
    // 在组装告警 API 和后台监控依赖时，使用共享连接池构造告警配置仓储。
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // 在告警设置 API 读取时于一致事务内获取 Token 和规则，只返回 Token 掩码。
    pub async fn get(&self, cluster_id: &str) -> Result<FeishuAlertSettings, RepositoryError> {
        // Token 和规则在同一读事务内取得，防止公开摘要混入并发保存前后的不同版本。
        let mut tx = self.pool.begin().await?;
        let token: Option<String> = sqlx::query_scalar("SELECT settings.access_token FROM clusters LEFT JOIN feishu_alert_settings settings ON settings.cluster_id = clusters.id WHERE clusters.id = ?")
            .bind(cluster_id).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::ClusterNotFound)?;
        let rules = sqlx::query_as("SELECT id, group_id, topic, lag_threshold, growth_seconds, enabled FROM feishu_alert_rules WHERE cluster_id = ? ORDER BY group_id, topic")
            .bind(cluster_id).fetch_all(&mut *tx).await?;
        // 取得完整公开设置所需的数据后结束读事务，再生成不含 Token 原值的响应。
        tx.commit().await?;
        Ok(public_settings(token.as_deref(), rules))
    }

    /// Save token changes and the complete rule collection in a single transaction.
    // 在告警设置 API 保存时原子替换 Token 和完整规则集合，失败回滚以保留旧配置。
    pub async fn replace(
        &self,
        cluster_id: &str,
        access_token: Option<&str>,
        clear_access_token: bool,
        rules: &[FeishuAlertRuleInput],
    ) -> Result<FeishuAlertSettings, RepositoryError> {
        // 将旧 Token 合并和整批规则替换放在同一事务内；校验或写入失败时不提交。
        let mut tx = self.pool.begin().await?;
        let old_token: Option<String> = sqlx::query_scalar("SELECT settings.access_token FROM clusters LEFT JOIN feishu_alert_settings settings ON settings.cluster_id = clusters.id WHERE clusters.id = ?")
            .bind(cluster_id).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::ClusterNotFound)?;
        let new_token = access_token
            .map(str::trim)
            .filter(|token| !token.is_empty());
        if clear_access_token && new_token.is_some() {
            return Err(RepositoryError::InvalidInput);
        }
        // 先处理新值与清除冲突，再合并旧 Token；留空只保留秘密，不能意外清空后台通知凭据。
        let token = if clear_access_token {
            None
        } else {
            new_token.map(str::to_owned).or(old_token)
        };
        let existing: Vec<(String, String)> =
            sqlx::query_as("SELECT id, created_at FROM feishu_alert_rules WHERE cluster_id = ?")
                .bind(cluster_id)
                .fetch_all(&mut *tx)
                .await?;
        let existing: HashMap<_, _> = existing.into_iter().collect();
        let mut pairs = HashSet::new();
        let mut ids = HashSet::new();
        // 在删除旧规则前验证整批范围、Group/Topic 唯一性和已有 ID 归属，保证失败不破坏旧配置。
        for rule in rules {
            if rule.group_id.trim().is_empty()
                || rule.topic.trim().is_empty()
                || !(0..=MAX_SAFE_INTEGER).contains(&rule.lag_threshold)
                || !(MIN_GROWTH_SECONDS..=MAX_SAFE_INTEGER).contains(&rule.growth_seconds)
                || !pairs.insert((&rule.group_id, &rule.topic))
                || rule
                    .id
                    .as_ref()
                    .is_some_and(|id| !existing.contains_key(id) || !ids.insert(id))
            {
                return Err(RepositoryError::InvalidInput);
            }
        }
        // updated_at 同时作为后台监控 revision；每次整批保存同步更新，使规则或 Token 变化重建内存状态。
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO feishu_alert_settings (cluster_id, access_token, created_at, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(cluster_id) DO UPDATE SET access_token = excluded.access_token, updated_at = excluded.updated_at")
            .bind(cluster_id).bind(&token).bind(&now).bind(&now).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM feishu_alert_rules WHERE cluster_id = ?")
            .bind(cluster_id)
            .execute(&mut *tx)
            .await?;
        let mut saved = Vec::with_capacity(rules.len());
        for rule in rules {
            let id = rule
                .id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let created_at = existing.get(&id).unwrap_or(&now);
            sqlx::query("INSERT INTO feishu_alert_rules (id, cluster_id, group_id, topic, lag_threshold, growth_seconds, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(&id).bind(cluster_id).bind(&rule.group_id).bind(&rule.topic)
                .bind(rule.lag_threshold).bind(rule.growth_seconds).bind(rule.enabled)
                .bind(created_at).bind(&now).execute(&mut *tx).await?;
            saved.push(FeishuAlertRule {
                id,
                group_id: rule.group_id.clone(),
                topic: rule.topic.clone(),
                lag_threshold: rule.lag_threshold,
                growth_seconds: rule.growth_seconds,
                enabled: rule.enabled,
            });
        }
        // Token、revision 与所有规则写入成功后才提交，中途失败会回滚整套配置。
        tx.commit().await?;
        saved.sort_by(|left, right| {
            (&left.group_id, &left.topic).cmp(&(&right.group_id, &right.topic))
        });
        Ok(public_settings(token.as_deref(), saved))
    }

    /// Internal access for test notifications; never include this result in public output.
    // 在测试通知需要已保存 Token 时读取内部秘密，禁止将结果用于公开输出。
    pub async fn token(&self, cluster_id: &str) -> Result<Option<String>, RepositoryError> {
        let token: Option<String> = sqlx::query_scalar("SELECT settings.access_token FROM clusters LEFT JOIN feishu_alert_settings settings ON settings.cluster_id = clusters.id WHERE clusters.id = ?")
            .bind(cluster_id).fetch_optional(&self.pool).await?.ok_or(RepositoryError::ClusterNotFound)?;
        Ok(token.filter(|token| !token.trim().is_empty()))
    }

    /// Read eligible worker configurations and their enabled rules from one snapshot.
    // 在后台监控协调时从一致快照读取具备 Token 和启用规则的集群及 revision。
    pub async fn enabled_clusters(&self) -> Result<Vec<EnabledAlertCluster>, RepositoryError> {
        // 后台资格筛选与规则读取共享快照，避免监控依据旧 Token 搭配新规则启动。
        let mut tx = self.pool.begin().await?;
        let settings: Vec<(String, String, String)> = sqlx::query_as("SELECT cluster_id, access_token, updated_at FROM feishu_alert_settings WHERE access_token IS NOT NULL AND EXISTS (SELECT 1 FROM feishu_alert_rules rules WHERE rules.cluster_id = feishu_alert_settings.cluster_id AND rules.enabled = 1) ORDER BY cluster_id")
            .fetch_all(&mut *tx).await?;
        let mut clusters = Vec::with_capacity(settings.len());
        for (cluster_id, access_token, revision) in settings {
            if access_token.trim().is_empty() {
                continue;
            }
            let rules = sqlx::query_as("SELECT id, group_id, topic, lag_threshold, growth_seconds, enabled FROM feishu_alert_rules WHERE cluster_id = ? AND enabled = 1 ORDER BY group_id, topic")
                .bind(&cluster_id).fetch_all(&mut *tx).await?;
            // revision 与 Token、规则来自同一事务快照，协调器据此判断何时替换旧监控状态。
            clusters.push(EnabledAlertCluster {
                cluster_id,
                access_token,
                rules,
                revision,
            });
        }
        // 各集群资格、revision 和启用规则全部读取后结束事务，再交给协调器比较版本。
        tx.commit().await?;
        Ok(clusters)
    }
}

// 在告警配置查询或保存完成后构造公开设置，过滤空 Token 并只提供掩码。
fn public_settings(token: Option<&str>, rules: Vec<FeishuAlertRule>) -> FeishuAlertSettings {
    let token = token.filter(|token| !token.trim().is_empty());
    FeishuAlertSettings {
        access_token_configured: token.is_some(),
        masked_access_token: token.map(mask_token),
        rules,
    }
}

// 在生成公开告警摘要时遮蔽 Token，短 Token 不提供后缀以避免完整泄漏。
fn mask_token(token: &str) -> String {
    // Short tokens must never be disclosed in full, even as a masked suffix.
    let suffix: String = if token.chars().count() > 4 {
        token
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    } else {
        String::new()
    };
    format!("••••••••{suffix}")
}
