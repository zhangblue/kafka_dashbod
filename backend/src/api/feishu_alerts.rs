use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use axum::{
    Extension, Json,
    extract::{Path, rejection::JsonRejection},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    db::{clusters::ClusterRepository, feishu_alerts::FeishuAlertRepository},
    domain::feishu_alert::{FeishuAlertRuleInput, FeishuAlertSettings, MIN_GROWTH_SECONDS},
    error::ApiError,
    notifications::feishu::{AlertSender, validate_access_token},
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveSettings {
    access_token: Option<String>,
    #[serde(default)]
    clear_access_token: bool,
    rules: Vec<FeishuAlertRuleInput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestNotification {
    access_token: Option<String>,
}

pub async fn get(
    Extension(repository): Extension<FeishuAlertRepository>,
    Path(id): Path<String>,
) -> Result<Json<FeishuAlertSettings>, ApiError> {
    Ok(Json(repository.get(&id).await?))
}

pub async fn put(
    Extension(repository): Extension<FeishuAlertRepository>,
    Path(id): Path<String>,
    payload: Result<Json<SaveSettings>, JsonRejection>,
) -> Result<Json<FeishuAlertSettings>, ApiError> {
    let Json(mut input) = payload.map_err(ApiError::from_json)?;
    let mut fields = BTreeMap::new();
    let token = normalized_token(input.access_token.as_deref(), &mut fields);
    if token.is_some() && input.clear_access_token {
        fields.insert(
            "accessToken".into(),
            "A new token and clearing the token cannot be combined".into(),
        );
    }
    let mut pairs = HashSet::new();
    for (index, rule) in input.rules.iter_mut().enumerate() {
        rule.group_id = rule.group_id.trim().to_owned();
        rule.topic = rule.topic.trim().to_owned();
        let prefix = format!("rules.{index}");
        if rule.group_id.is_empty() {
            fields.insert(
                format!("{prefix}.groupId"),
                "Consumer group is required".into(),
            );
        }
        if rule.topic.is_empty() {
            fields.insert(format!("{prefix}.topic"), "Topic is required".into());
        }
        if !pairs.insert((rule.group_id.clone(), rule.topic.clone())) {
            fields.insert(
                format!("{prefix}.groupId"),
                "The group/topic pair must be unique".into(),
            );
        }
        if rule.lag_threshold < 0 {
            fields.insert(
                format!("{prefix}.lagThreshold"),
                "Must be zero or greater".into(),
            );
        }
        if rule.growth_seconds < MIN_GROWTH_SECONDS {
            fields.insert(
                format!("{prefix}.growthSeconds"),
                "Must be at least 10 seconds".into(),
            );
        }
    }
    if !fields.is_empty() {
        return Err(ApiError::validation(fields));
    }
    Ok(Json(
        repository
            .replace(&id, token, input.clear_access_token, &input.rules)
            .await?,
    ))
}

pub async fn test(
    Extension(repository): Extension<FeishuAlertRepository>,
    Extension(clusters): Extension<ClusterRepository>,
    Extension(sender): Extension<Arc<dyn AlertSender>>,
    Path(id): Path<String>,
    payload: Result<Json<TestNotification>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Json(input) = payload.map_err(ApiError::from_json)?;
    let mut fields = BTreeMap::new();
    let draft = normalized_token(input.access_token.as_deref(), &mut fields);
    if !fields.is_empty() {
        return Err(ApiError::validation(fields));
    }
    let cluster = clusters
        .get(&id)
        .await?
        .ok_or_else(ApiError::cluster_not_found)?;
    let stored;
    let token = if let Some(token) = draft {
        token
    } else {
        stored = repository.token(&id).await?;
        stored.as_deref().ok_or_else(|| {
            ApiError::validation(BTreeMap::from([(
                "accessToken".into(),
                "An access token is required".into(),
            )]))
        })?
    };
    let text = format!("飞书通知测试\n集群：{}", cluster.name);
    sender.send_text(token, &text).await.map_err(|_| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "FEISHU_SEND_FAILED",
            "Feishu notification could not be sent",
        )
    })?;
    Ok(Json(json!({"ok":true})))
}

fn normalized_token<'a>(
    token: Option<&'a str>,
    fields: &mut BTreeMap<String, String>,
) -> Option<&'a str> {
    let token = token?;
    if token.trim().is_empty() && !token.chars().any(char::is_control) {
        return None;
    }
    match validate_access_token(token) {
        Ok(token) => Some(token),
        Err(_) => {
            fields.insert(
                "accessToken".into(),
                "Token must be at most 512 bytes and contain no control characters".into(),
            );
            None
        }
    }
}
