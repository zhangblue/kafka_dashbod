use std::collections::BTreeMap;

use axum::{
    Extension, Json,
    extract::{Path, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::{
    domain::topic_admin::{
        CreateTopicInput, TopicAdminError, TopicConfigUpdateOutcome, TopicConfigValue,
        UpdateTopicConfigsInput, validate_delete,
    },
    error::ApiError,
    sessions::manager::{SessionClientFactory, SessionManager, TopicOperationError},
};

#[derive(Serialize)]
pub struct TopicConfigsResponse {
    configs: Vec<TopicConfigValue>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum TopicMutationResult {
    Created,
    AppliedButUnconfirmed,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeleteTopicInput {
    confirmation_name: String,
}

pub async fn create<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
    payload: Result<Json<CreateTopicInput>, JsonRejection>,
) -> Result<Response, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    let result: Result<_, ApiError> = async {
        let Json(input) = payload.map_err(ApiError::from_json)?;
        input.validate().map_err(ApiError::validation)?;
        sessions.create_topic(&id, input).await?;
        Ok((StatusCode::CREATED, Json(TopicMutationResult::Created)).into_response())
    }
    .await;
    result.map_err(|error| error.with_cluster_id(cluster_id))
}

pub async fn configs<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path((id, topic)): Path<(String, String)>,
) -> Result<Json<TopicConfigsResponse>, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    let result: Result<_, ApiError> = async {
        validate_path_topic(&topic)?;
        let configs = sessions.describe_topic_configs(&id, &topic).await?;
        Ok(Json(TopicConfigsResponse { configs }))
    }
    .await;
    result.map_err(|error| error.with_cluster_id(cluster_id))
}

pub async fn update<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path((id, topic)): Path<(String, String)>,
    payload: Result<Json<UpdateTopicConfigsInput>, JsonRejection>,
) -> Result<Response, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    let result: Result<_, ApiError> = async {
        validate_path_topic(&topic)?;
        let Json(input) = payload.map_err(ApiError::from_json)?;
        input.validate().map_err(ApiError::validation)?;
        Ok(
            match sessions.update_topic_configs(&id, &topic, input).await? {
                TopicConfigUpdateOutcome::Confirmed(configs) => {
                    Json(TopicConfigsResponse { configs }).into_response()
                }
                TopicConfigUpdateOutcome::AppliedButUnconfirmed => (
                    StatusCode::ACCEPTED,
                    Json(TopicMutationResult::AppliedButUnconfirmed),
                )
                    .into_response(),
            },
        )
    }
    .await;
    result.map_err(|error| error.with_cluster_id(cluster_id))
}

pub async fn delete<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path((id, topic)): Path<(String, String)>,
    payload: Result<Json<DeleteTopicInput>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    let result: Result<_, ApiError> = async {
        let Json(input) = payload.map_err(ApiError::from_json)?;
        validate_delete(&topic, &input.confirmation_name)?;
        sessions.delete_topic(&id, &topic).await?;
        Ok(StatusCode::NO_CONTENT)
    }
    .await;
    result.map_err(|error| error.with_cluster_id(cluster_id))
}

fn validate_path_topic(topic: &str) -> Result<(), ApiError> {
    // Reuse the domain name contract without imposing deletion's internal-topic restriction.
    CreateTopicInput {
        name: topic.into(),
        partitions: 1,
        replication_factor: None,
        configs: BTreeMap::new(),
    }
    .validate()
    .map_err(ApiError::validation)
}

impl From<TopicOperationError> for ApiError {
    fn from(error: TopicOperationError) -> Self {
        match error {
            TopicOperationError::Session(error) => error.into(),
            TopicOperationError::Admin(error) => error.into(),
        }
    }
}

impl From<TopicAdminError> for ApiError {
    fn from(error: TopicAdminError) -> Self {
        let status = match &error {
            TopicAdminError::Validation(fields) => return Self::validation(fields.clone()),
            TopicAdminError::InternalTopicProtected | TopicAdminError::Authorization => {
                StatusCode::FORBIDDEN
            }
            TopicAdminError::AlreadyExists | TopicAdminError::ConfigChanged => StatusCode::CONFLICT,
            TopicAdminError::NotFound => StatusCode::NOT_FOUND,
            TopicAdminError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            TopicAdminError::InvalidConfig => StatusCode::UNPROCESSABLE_ENTITY,
            TopicAdminError::Unavailable => StatusCode::BAD_GATEWAY,
        };
        Self::new(status, error.code(), &error.to_string())
    }
}
