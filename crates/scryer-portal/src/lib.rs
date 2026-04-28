//! scryer-portal — local-first management portal backend.
//!
//! Exposes an axum HTTP API for two domains: launchd-managed scryer fetcher
//! jobs (read-only inspection + run/load/unload control) and the parquet
//! dataset under `dataset/` (DuckDB-backed query + export).
//!
//! Run via `cargo run -p scryer-portal --bin scryer-portal-server`. Same
//! binary is bundled as a Tauri sidecar by `scryer-portal-shell`.

pub mod api;
pub mod config;
pub mod data;
pub mod error;
pub mod jobs;
pub mod state;

pub use config::PortalConfig;
pub use error::{ApiError, ApiResult};
pub use state::AppState;

use std::sync::Arc;

use axum::Router;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// Build the full axum router for the portal API. Wired with permissive CORS
/// (the portal binds to localhost or sits behind an IP-allowlisted proxy; CORS
/// is not the security boundary).
pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .nest("/api", api::router())
        .with_state(state)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
}
