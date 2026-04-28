use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::jobs::{JobDetail, JobSummary};
use crate::state::AppState;

pub async fn list(State(state): State<Arc<AppState>>) -> ApiResult<Json<Vec<JobSummary>>> {
    Ok(Json(state.jobs.list().await?))
}

pub async fn detail(
    State(state): State<Arc<AppState>>,
    Path(label): Path<String>,
) -> ApiResult<Json<JobDetail>> {
    Ok(Json(state.jobs.get(&label).await?))
}

pub async fn run(
    State(state): State<Arc<AppState>>,
    Path(label): Path<String>,
) -> ApiResult<Json<Value>> {
    state.jobs.run(&label).await?;
    Ok(Json(json!({"ok": true})))
}

pub async fn load(
    State(state): State<Arc<AppState>>,
    Path(label): Path<String>,
) -> ApiResult<Json<Value>> {
    state.jobs.load(&label).await?;
    Ok(Json(json!({"ok": true})))
}

pub async fn unload(
    State(state): State<Arc<AppState>>,
    Path(label): Path<String>,
) -> ApiResult<Json<Value>> {
    state.jobs.unload(&label).await?;
    Ok(Json(json!({"ok": true})))
}
