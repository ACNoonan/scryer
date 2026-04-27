use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use thiserror::Error;

/// Errors surfaced to JSON-RPC clients. Each variant carries the right
/// HTTP status code + JSON-RPC error code; the upstream provider is
/// never exposed to the client.
#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("invalid payload: {0}")]
    InvalidPayload(String),

    #[error("method `{0}` is not allowed (mutating)")]
    MutatingMethod(String),

    #[error("no healthy providers available")]
    NoHealthyProviders,

    #[error("upstream request failed: {0}")]
    Upstream(String),
}

impl ProxyError {
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::InvalidPayload(_) => StatusCode::BAD_REQUEST,
            Self::MutatingMethod(_) => StatusCode::FORBIDDEN,
            Self::NoHealthyProviders => StatusCode::SERVICE_UNAVAILABLE,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
        }
    }

    pub fn jsonrpc_code(&self) -> i64 {
        match self {
            Self::InvalidPayload(_) => -32000,
            Self::MutatingMethod(_) => -32601,
            Self::NoHealthyProviders => -32100,
            Self::Upstream(_) => -32001,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": serde_json::Value::Null,
            "error": {
                "code": self.jsonrpc_code(),
                "message": self.to_string(),
            }
        });
        (self.http_status(), Json(body)).into_response()
    }
}

/// Errors raised inside the proxy machinery (config loading, registry
/// validation). These never reach a JSON-RPC client.
#[derive(Debug, Error)]
pub enum InitError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("config parse error: {0}")]
    Config(#[from] serde_json::Error),

    #[error("invalid provider config: {0}")]
    InvalidProvider(String),

    #[error("environment variable `{0}` referenced in config but not set")]
    MissingEnv(String),

    #[error("metrics registration failed: {0}")]
    Metrics(#[from] prometheus::Error),
}
