use axum::{Extension, Json, extract::Path, http::StatusCode};
use serde::Serialize;

use crate::{
    db::clusters::ClusterRepository,
    error::ApiError,
    kafka::client::ConnectionError,
    sessions::manager::{SessionClientFactory, SessionError, SessionManager},
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedSession {
    pub session_id: String,
    pub lease_seconds: u64,
}

// 处理集群监控会话创建请求，读取有效配置后按需创建带短租约的 Kafka 客户端。
pub async fn create<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<CreatedSession>), ApiError> {
    // 读取配置和创建会话持有同一集群操作锁，避免与集群删除交错而留下孤立会话。
    let _operation = sessions.cluster_operation().await;
    let config = repository
        .decrypted_config(&id)
        .await?
        .ok_or_else(ApiError::cluster_not_found)?;
    let session_id = sessions.create(id, config).await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedSession {
            session_id,
            lease_seconds: sessions.lease_seconds(),
        }),
    ))
}

// 处理会话关闭请求，页面离开或切换集群时等待已接受工作及客户端资源释放。
pub async fn delete<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    // 必须等待关闭完成后返回，不能把在途采集、管理操作或原生析构遗留到后台。
    sessions
        .close(&id)
        .await
        .map_err(|error| ApiError::from(error).with_cluster_id(cluster_id))?;
    Ok(StatusCode::NO_CONTENT)
}

// 处理会话快照请求，由管理器续租并执行互斥采集，错误附带对应集群标识。
pub async fn snapshot<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<Json<crate::domain::snapshot::ClusterSnapshot>, ApiError> {
    let cluster_id = sessions.cluster_id(&id).await;
    Ok(Json(sessions.snapshot(&id).await.map_err(|error| {
        ApiError::from(error).with_cluster_id(cluster_id)
    })?))
}

impl From<SessionError> for ApiError {
    // 在会话操作失败时将有限错误类型转换为统一安全响应，不暴露原生 Kafka 错误。
    fn from(error: SessionError) -> Self {
        let (status, code) = match error {
            SessionError::Expired => (StatusCode::GONE, "SESSION_EXPIRED"),
            SessionError::CollectionInProgress => (StatusCode::CONFLICT, "SNAPSHOT_IN_PROGRESS"),
            SessionError::ShuttingDown => (StatusCode::SERVICE_UNAVAILABLE, "SHUTTING_DOWN"),
            SessionError::Connection(error) => (
                if error == ConnectionError::InvalidConfig {
                    StatusCode::UNPROCESSABLE_ENTITY
                } else {
                    StatusCode::BAD_GATEWAY
                },
                error.code(),
            ),
        };
        Self::new(status, code, &error.to_string())
    }
}
