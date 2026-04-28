use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("not implemented on this platform: {0}")]
    NotImplemented(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            ApiError::NotImplemented(m) => (StatusCode::NOT_IMPLEMENTED, m.clone()),
            ApiError::Anyhow(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
            ApiError::Json(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")),
        };
        if matches!(self, ApiError::Anyhow(_) | ApiError::Json(_)) {
            tracing::error!(error = %self, "api error");
        }
        (status, Json(json!({ "error": msg }))).into_response()
    }
}
