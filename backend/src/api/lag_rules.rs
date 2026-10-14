use std::collections::{BTreeMap, HashSet};

use axum::{
    Extension, Json,
    extract::{Path, rejection::JsonRejection},
};
use serde::{Deserialize, Serialize};

use crate::{db::lag_rules::LagRuleRepository, domain::lag_rule::LagRuleInput, error::ApiError};

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Thresholds {
    normal_max: i64,
    high_min: i64,
    over_limit_seconds: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Override {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_id: Option<String>,
    group_id: String,
    topic: String,
    #[serde(flatten)]
    thresholds: Thresholds,
}

#[derive(Deserialize, Serialize)]
pub struct LagRules {
    defaults: Thresholds,
    overrides: Vec<Override>,
}

pub async fn get(
    Extension(repository): Extension<LagRuleRepository>,
    Path(id): Path<String>,
) -> Result<Json<LagRules>, ApiError> {
    let (defaults, rules) = repository.settings(&id).await?;
    Ok(Json(from_stored(defaults, rules)))
}

pub async fn put(
    Extension(repository): Extension<LagRuleRepository>,
    Path(id): Path<String>,
    payload: Result<Json<LagRules>, JsonRejection>,
) -> Result<Json<LagRules>, ApiError> {
    let Json(input) = payload.map_err(ApiError::from_json)?;
    let mut fields = BTreeMap::new();
    let defaults = &input.defaults;
    validate_thresholds(
        &mut fields,
        "defaults.normalMax",
        "defaults.highMin",
        "defaults.overLimitSeconds",
        defaults.normal_max,
        defaults.high_min,
        defaults.over_limit_seconds,
    );
    let mut unique = HashSet::new();
    for (index, rule) in input.overrides.iter().enumerate() {
        let prefix = format!("overrides.{index}");
        if rule.group_id.trim().is_empty() {
            fields.insert(
                format!("{prefix}.groupId"),
                "Consumer group is required".into(),
            );
        }
        if rule.topic.trim().is_empty() {
            fields.insert(format!("{prefix}.topic"), "Topic is required".into());
        }
        if !unique.insert((&rule.group_id, &rule.topic)) {
            fields.insert(
                format!("{prefix}.groupId"),
                "The group/topic pair must be unique".into(),
            );
        }
        validate_thresholds(
            &mut fields,
            &format!("{prefix}.normalMax"),
            &format!("{prefix}.highMin"),
            &format!("{prefix}.overLimitSeconds"),
            rule.thresholds.normal_max,
            rule.thresholds.high_min,
            rule.thresholds.over_limit_seconds,
        );
    }
    if !fields.is_empty() {
        return Err(ApiError::validation(fields));
    }
    let rules: Vec<_> = input
        .overrides
        .into_iter()
        .map(|rule| LagRuleInput {
            group_id: rule.group_id,
            topic: rule.topic,
            normal_lag_max: rule.thresholds.normal_max,
            high_lag_min: rule.thresholds.high_min,
            over_limit_seconds: rule.thresholds.over_limit_seconds,
        })
        .collect();
    let saved = repository
        .replace_settings(
            &id,
            (
                defaults.normal_max,
                defaults.high_min,
                defaults.over_limit_seconds,
            ),
            &rules,
        )
        .await?;
    Ok(Json(from_stored(
        (
            defaults.normal_max,
            defaults.high_min,
            defaults.over_limit_seconds,
        ),
        saved,
    )))
}

fn from_stored(
    defaults: (i64, i64, i64),
    rules: Vec<crate::domain::lag_rule::LagRule>,
) -> LagRules {
    LagRules {
        defaults: Thresholds {
            normal_max: defaults.0,
            high_min: defaults.1,
            over_limit_seconds: defaults.2,
        },
        overrides: rules
            .into_iter()
            .map(|rule| Override {
                id: Some(rule.id),
                cluster_id: Some(rule.cluster_id),
                group_id: rule.group_id,
                topic: rule.topic,
                thresholds: Thresholds {
                    normal_max: rule.normal_lag_max,
                    high_min: rule.high_lag_min,
                    over_limit_seconds: rule.over_limit_seconds,
                },
            })
            .collect(),
    }
}

pub(super) fn validate_thresholds(
    fields: &mut BTreeMap<String, String>,
    normal_field: &str,
    high_field: &str,
    duration_field: &str,
    normal: i64,
    high: i64,
    duration: i64,
) {
    if normal < 0 {
        fields.insert(normal_field.into(), "Must be zero or greater".into());
    }
    if high <= normal {
        fields.insert(
            high_field.into(),
            "Must be greater than the normal maximum".into(),
        );
    }
    if duration < 0 {
        fields.insert(duration_field.into(), "Must be zero or greater".into());
    }
}
