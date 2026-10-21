use super::RepositoryError;
use crate::domain::feishu_alert::{
    EnabledAlertCluster, FeishuAlertRule, FeishuAlertRuleInput, FeishuAlertSettings,
    MIN_GROWTH_SECONDS,
};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct FeishuAlertRepository {
    pool: SqlitePool,
}

impl FeishuAlertRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn get(&self, cluster_id: &str) -> Result<FeishuAlertSettings, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let token: Option<String> = sqlx::query_scalar("SELECT settings.access_token FROM clusters LEFT JOIN feishu_alert_settings settings ON settings.cluster_id = clusters.id WHERE clusters.id = ?")
            .bind(cluster_id).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::ClusterNotFound)?;
        let rules = sqlx::query_as("SELECT id, group_id, topic, lag_threshold, growth_seconds, enabled FROM feishu_alert_rules WHERE cluster_id = ? ORDER BY group_id, topic")
            .bind(cluster_id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(public_settings(token.as_deref(), rules))
    }

    /// Save token changes and the complete rule collection in a single transaction.
    pub async fn replace(
        &self,
        cluster_id: &str,
        access_token: Option<&str>,
        clear_access_token: bool,
        rules: &[FeishuAlertRuleInput],
    ) -> Result<FeishuAlertSettings, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let old_token: Option<String> = sqlx::query_scalar("SELECT settings.access_token FROM clusters LEFT JOIN feishu_alert_settings settings ON settings.cluster_id = clusters.id WHERE clusters.id = ?")
            .bind(cluster_id).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::ClusterNotFound)?;
        let new_token = access_token
            .map(str::trim)
            .filter(|token| !token.is_empty());
        if clear_access_token && new_token.is_some() {
            return Err(RepositoryError::InvalidInput);
        }
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
        for rule in rules {
            if rule.group_id.trim().is_empty()
                || rule.topic.trim().is_empty()
                || rule.lag_threshold < 0
                || rule.growth_seconds < MIN_GROWTH_SECONDS
                || !pairs.insert((&rule.group_id, &rule.topic))
                || rule
                    .id
                    .as_ref()
                    .is_some_and(|id| !existing.contains_key(id) || !ids.insert(id))
            {
                return Err(RepositoryError::InvalidInput);
            }
        }
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
        tx.commit().await?;
        saved.sort_by(|left, right| {
            (&left.group_id, &left.topic).cmp(&(&right.group_id, &right.topic))
        });
        Ok(public_settings(token.as_deref(), saved))
    }

    /// Internal access for test notifications; never include this result in public output.
    pub async fn token(&self, cluster_id: &str) -> Result<Option<String>, RepositoryError> {
        let token: Option<String> = sqlx::query_scalar("SELECT settings.access_token FROM clusters LEFT JOIN feishu_alert_settings settings ON settings.cluster_id = clusters.id WHERE clusters.id = ?")
            .bind(cluster_id).fetch_optional(&self.pool).await?.ok_or(RepositoryError::ClusterNotFound)?;
        Ok(token.filter(|token| !token.trim().is_empty()))
    }

    /// Read eligible worker configurations and their enabled rules from one snapshot.
    pub async fn enabled_clusters(&self) -> Result<Vec<EnabledAlertCluster>, RepositoryError> {
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
            clusters.push(EnabledAlertCluster {
                cluster_id,
                access_token,
                rules,
                revision,
            });
        }
        tx.commit().await?;
        Ok(clusters)
    }
}

fn public_settings(token: Option<&str>, rules: Vec<FeishuAlertRule>) -> FeishuAlertSettings {
    let token = token.filter(|token| !token.trim().is_empty());
    FeishuAlertSettings {
        access_token_configured: token.is_some(),
        masked_access_token: token.map(mask_token),
        rules,
    }
}

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
