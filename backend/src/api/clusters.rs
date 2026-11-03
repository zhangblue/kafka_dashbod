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

// 处理集群列表请求，设置页面加载时返回不含凭据内容的配置摘要。
pub async fn list(
    Extension(repository): Extension<ClusterRepository>,
) -> Result<Json<Vec<ClusterDto>>, ApiError> {
    Ok(Json(repository.list().await?))
}

// 处理单个集群查询，编辑配置前读取摘要并区分不存在的集群。
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

// 处理新增集群请求，在解析和校验通过后保存配置，不创建 Kafka 会话。
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

// 处理集群更新请求，在表单校验后交由仓储合并保留或清除的凭据。
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

// 处理集群删除请求，等待相关会话和在途工作释放后再删除持久化配置。
pub async fn delete<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    // 与会话创建、连接测试共享集群操作锁，防止删除期间又接受引用该配置的新工作。
    let _operation = sessions.cluster_operation().await;
    repository
        .get(&id)
        .await?
        .ok_or_else(ApiError::cluster_not_found)?;
    // 先等待该集群会话、临时测试和已接受的管理工作释放，再删除其配置。
    sessions.close_for_cluster(&id).await;
    if !repository.delete(&id).await? {
        return Err(ApiError::cluster_not_found());
    }
    Ok(StatusCode::NO_CONTENT)
}

// 处理已保存集群的连接测试，兼容空请求体，也支持复用已存凭据的编辑草稿。
pub async fn test_connection<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Result<Json<ConnectionTestResult>, ApiError> {
    let body = payload.map_err(|error| ApiError::from_json(error.into()))?;
    // 即使客户端声明 JSON 类型，空请求体仍兼容测试已保存配置。
    let request = if body.is_empty() {
        None
    } else {
        // 草稿复用已存凭据前先复用 JSON 媒体类型校验；此前的 Bytes 提取已执行请求体上限检查。
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
    // 临时测试交给管理器跟踪，确保删除集群或退出时可以等待原生客户端释放。
    Ok(Json(sessions.test_connection(Some(id), config).await?))
}

// 处理新增草稿的连接测试，保存前临时验证配置且不创建监控会话。
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

// 在集群保存或草稿测试前校验必填字段、超时及默认 Lag 阈值。
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
