use std::collections::BTreeMap;

use axum::{
    Extension, Json,
    body::{Body, Bytes},
    extract::{
        FromRequest, Path, Request,
        rejection::{BytesRejection, JsonRejection},
    },
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;

use crate::{
    db::clusters::ClusterRepository,
    domain::cluster::{ClusterDto, ClusterInput, SecretClears},
    error::ApiError,
    kafka::client::ConnectionTestResult,
    sessions::manager::{SessionClientFactory, SessionManager},
};

#[derive(Deserialize)]
pub struct ClusterRequest {
    #[serde(flatten)]
    input: ClusterInput,
    #[serde(flatten)]
    clears: SecretClears,
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
        Json(
            repository
                .create_with_secret_clears(&request.input, request.clears)
                .await?,
        ),
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
            .update_with_secret_clears(&id, &request.input, request.clears)
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

pub async fn test_connection<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Result<Json<ConnectionTestResult>, ApiError> {
    let body = payload.map_err(|error| ApiError::from_json(error.into()))?;
    // Empty bodies remain compatible even if a client sends a JSON content type.
    let request = if body.is_empty() {
        None
    } else {
        // Reuse Axum's media-type checks before a draft can reuse saved secrets.
        // The original Bytes extractor has already enforced the request body limit.
        let mut request = Request::new(Body::from(body));
        *request.headers_mut() = headers;
        Some(
            Json::<ClusterRequest>::from_request(request, &())
                .await
                .map_err(ApiError::from_json)?
                .0,
        )
    };
    let _operation = sessions.cluster_operation().await;
    let config = if let Some(request) = request {
        validate(&request.input)?;
        repository
            .draft_config(Some(&id), &request.input, request.clears)
            .await?
    } else {
        repository.decrypted_config(&id).await?
    }
    .ok_or_else(ApiError::cluster_not_found)?;
    Ok(Json(sessions.test_connection(Some(id), config).await?))
}

pub async fn test_draft_connection<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    payload: Result<Json<ClusterRequest>, JsonRejection>,
) -> Result<Json<ConnectionTestResult>, ApiError> {
    let Json(request) = payload.map_err(ApiError::from_json)?;
    validate(&request.input)?;
    let config = repository
        .draft_config(None, &request.input, request.clears)
        .await?
        .ok_or_else(ApiError::cluster_not_found)?;
    Ok(Json(sessions.test_connection(None, config).await?))
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
