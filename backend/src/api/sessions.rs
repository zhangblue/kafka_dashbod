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

pub async fn create<F: SessionClientFactory>(
    Extension(repository): Extension<ClusterRepository>,
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<CreatedSession>), ApiError> {
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

pub async fn delete<F: SessionClientFactory>(
    Extension(sessions): Extension<SessionManager<F>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    sessions.close(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

impl From<SessionError> for ApiError {
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
