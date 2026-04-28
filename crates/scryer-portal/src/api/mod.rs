use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use crate::state::AppState;

pub mod data;
pub mod jobs;
pub mod misc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(misc::health))
        .route("/jobs", get(jobs::list))
        .route("/jobs/:label", get(jobs::detail))
        .route("/jobs/:label/run", post(jobs::run))
        .route("/jobs/:label/load", post(jobs::load))
        .route("/jobs/:label/unload", post(jobs::unload))
        .route("/datasets", get(data::list_datasets))
        .route("/datasets/:venue/:data_type/:version/preview", get(data::preview))
        .route("/query", post(data::query))
        .route("/export", post(data::export))
}
