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

// 处理 Topic 创建请求，在校验通过后通过当前有效会话执行受控管理操作。
pub async fn create<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
    payload: Result<Json<CreateTopicInput>, JsonRejection>,
) -> Result<Response, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    let result: Result<_, ApiError> = async {
        let Json(input) = payload.map_err(ApiError::from_json)?;
        input.validate().map_err(ApiError::validation)?;
        // 复用会话 AdminClient 及采集互斥；管理器跟踪已接受工作以供关闭时等待。
        sessions.create_topic(&id, input).await?;
        Ok((StatusCode::CREATED, Json(TopicMutationResult::Created)).into_response())
    }
    .await;
    result.map_err(|error| error.with_cluster_id(cluster_id))
}

// 处理 Topic 配置查询，弹窗打开时通过当前会话实时读取值及来源，不使用配置副本。
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

// 处理 Topic 配置保存，通过会话执行冲突检查和增量修改，保留已应用但未确认的语义。
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
                // 写入已被接受但重读未确认时返回 202，避免误导调用者按失败重复写入。
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

// 处理 Topic 删除请求，先核对完整确认名称并保护内部 Topic，再调用会话管理操作。
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

// 在配置读取和修改前复用领域名称校验，避免对非删除操作施加内部 Topic 删除限制。
fn validate_path_topic(topic: &str) -> Result<(), ApiError> {
    // 仅复用名称约束，读取或修改配置不沿用删除接口的内部 Topic 禁止规则。
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
    // 在 Topic 操作失败时分别复用会话或管理错误映射，维持统一安全错误结构。
    fn from(error: TopicOperationError) -> Self {
        match error {
            TopicOperationError::Session(error) => error.into(),
            TopicOperationError::Admin(error) => error.into(),
        }
    }
}

impl From<TopicAdminError> for ApiError {
    // 在管理操作失败时映射校验、权限、冲突和超时状态，只返回领域层安全消息。
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
