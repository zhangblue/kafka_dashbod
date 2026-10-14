use std::collections::BTreeMap;

use axum::{
    Json,
    body::Body,
    extract::{Request, rejection::JsonRejection},
    http::{
        HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::db::RepositoryError;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

pub struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: &str) -> Self {
        Self {
            status,
            body: ErrorBody {
                code,
                message: message.into(),
                details: None,
                request_id: None,
            },
        }
    }

    pub fn validation(details: BTreeMap<String, String>) -> Self {
        let mut error = Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "VALIDATION_ERROR",
            "The request contains invalid fields",
        );
        error.body.details = Some(details);
        error
    }

    pub fn cluster_not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "CLUSTER_NOT_FOUND",
            "Cluster not found",
        )
    }

    pub fn from_json(_: JsonRejection) -> Self {
        // Rejection messages can include user-provided values, including secrets.
        Self::validation(BTreeMap::from([(
            "body".into(),
            "A valid JSON request with the required fields is expected".into(),
        )]))
    }
}

impl From<RepositoryError> for ApiError {
    fn from(error: RepositoryError) -> Self {
        match error {
            RepositoryError::ClusterNotFound => Self::cluster_not_found(),
            RepositoryError::InvalidInput => {
                Self::validation(BTreeMap::from([("body".into(), "Invalid settings".into())]))
            }
            RepositoryError::Database(ref error)
                if error
                    .as_database_error()
                    .is_some_and(|error| error.is_unique_violation()) =>
            {
                Self::new(
                    StatusCode::CONFLICT,
                    "CONFLICT",
                    "A configuration with the same unique fields already exists",
                )
            }
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "The request could not be completed",
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Never log SQL errors, request payloads, or decrypted configuration.
        tracing::warn!(code = self.body.code, "API request failed");
        let mut response = (self.status, Json(self.body.clone())).into_response();
        response.extensions_mut().insert(self.body);
        response
    }
}

pub async fn request_id(request: Request, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-request-id")
        .filter(|value| value.to_str().is_ok_and(|value| !value.is_empty()))
        .cloned()
        .unwrap_or_else(|| {
            HeaderValue::from_str(&uuid::Uuid::new_v4().to_string())
                .expect("UUID is a valid header")
        });
    let mut response = next.run(request).await;
    let error = response.extensions_mut().remove::<ErrorBody>().or_else(|| {
        // Extractors may reject a request before its handler can return ApiError.
        let status = response.status();
        if status.is_client_error() {
            Some(
                ApiError::new(
                    status,
                    "VALIDATION_ERROR",
                    "The request could not be processed",
                )
                .body,
            )
        } else if status.is_server_error() {
            Some(
                ApiError::new(
                    status,
                    "INTERNAL_ERROR",
                    "The request could not be completed",
                )
                .body,
            )
        } else {
            None
        }
    });
    if let Some(mut error) = error {
        error.request_id = id.to_str().ok().map(str::to_owned);
        *response.body_mut() =
            Body::from(serde_json::to_vec(&error).expect("ErrorBody is serializable"));
        response.headers_mut().remove(CONTENT_LENGTH);
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    response.headers_mut().insert("x-request-id", id);
    response
}

pub async fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "NOT_FOUND", "Route not found")
}

pub async fn method_not_allowed() -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "METHOD_NOT_ALLOWED",
        "Method not allowed",
    )
}
