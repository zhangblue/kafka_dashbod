use std::collections::BTreeMap;

use axum::{
    Extension, Json,
    extract::{Path, rejection::JsonRejection},
    http::StatusCode,
};
use serde::Deserialize;

use crate::{
    db::clusters::ClusterRepository,
    domain::cluster::{ClusterDto, ClusterInput},
    error::ApiError,
    kafka::client::{ConnectionError, ConnectionTestResult},
    sessions::manager::{SessionClientFactory, SessionManager},
};

#[derive(Deserialize)]
pub struct ClusterRequest {
    #[serde(flatten)]
    input: ClusterInput,
    #[serde(default, rename = "clearPassword")]
    clear_password: bool,
}

pub async fn list(
    Extension(repository): Extension<ClusterRepository>,
) -> Result<Json<Vec<ClusterDto>>, ApiError> {
    Ok(Json(repository.list().await?))
}

pub async fn get(
    Extension(repository): Extension<ClusterRepository>,
    Path(id): Path<String>,
) -> Result<Json<ClusterDto>, ApiError> {
    Ok(Json(
        repository
            .get(&id)
            .await?
            .ok_or_else(ApiError::cluster_not_found)?,
    ))
}

pub async fn create(
    Extension(repository): Extension<ClusterRepository>,
    payload: Result<Json<ClusterRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ClusterDto>), ApiError> {
    let Json(request) = payload.map_err(ApiError::from_json)?;
    validate(&request.input)?;
    Ok((
        StatusCode::CREATED,
        Json(repository.create(&request.input).await?),
    ))
}

pub async fn update(
    Extension(repository): Extension<ClusterRepository>,
    Path(id): Path<String>,
    payload: Result<Json<ClusterRequest>, JsonRejection>,
) -> Result<Json<ClusterDto>, ApiError> {
    let Json(request) = payload.map_err(ApiError::from_json)?;
    validate(&request.input)?;
    Ok(Json(
        repository
            .update_with_password_clear(&id, &request.input, request.clear_password)
            .await?
            .ok_or_else(ApiError::cluster_not_found)?,
    ))
}

pub async fn delete<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let _operation = sessions.cluster_operation().await;
    repository
        .get(&id)
        .await?
        .ok_or_else(ApiError::cluster_not_found)?;
    sessions.close_for_cluster(&id).await;
    if !repository.delete(&id).await? {
        return Err(ApiError::cluster_not_found());
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn test_connection(
    Extension(repository): Extension<ClusterRepository>,
    Path(id): Path<String>,
) -> Result<Json<ConnectionTestResult>, ApiError> {
    let config = repository
        .decrypted_config(&id)
        .await?
        .ok_or_else(ApiError::cluster_not_found)?;
    crate::kafka::client::test_connection(config)
        .await
        .map(Json)
        .map_err(|error| {
            let status = if error == ConnectionError::InvalidConfig {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::BAD_GATEWAY
            };
            ApiError::new(status, error.code(), &error.to_string())
        })
}

fn validate(input: &ClusterInput) -> Result<(), ApiError> {
    let mut fields = BTreeMap::new();
    if input.name.trim().is_empty() {
        fields.insert("name".into(), "Name is required".into());
    }
    if input.bootstrap_servers.trim().is_empty() {
        fields.insert(
            "bootstrapServers".into(),
            "Bootstrap servers are required".into(),
        );
    }
    if input.connection_timeout_ms <= 0 {
        fields.insert(
            "connectionTimeoutMs".into(),
            "Must be greater than zero".into(),
        );
    }
    super::lag_rules::validate_thresholds(
        &mut fields,
        "defaultNormalLagMax",
        "defaultHighLagMin",
        "defaultOverLimitSeconds",
        input.default_normal_lag_max,
        input.default_high_lag_min,
        input.default_over_limit_seconds,
    );
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(fields))
    }
}
