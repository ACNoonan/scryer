use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

use crate::data::{discovery, export as exporter, query as query_engine};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

pub async fn list_datasets(
    State(state): State<Arc<AppState>>,
) -> ApiResult<Json<Value>> {
    let schemas = discovery::discover(&state.cfg.dataset_root)?;
    Ok(Json(serde_json::json!({
        "dataset_root": state.cfg.dataset_root.to_string_lossy(),
        "schemas": schemas,
    })))
}

#[derive(Debug, Deserialize)]
pub struct PreviewParams {
    #[serde(default = "default_preview_limit")]
    pub limit: usize,
}
fn default_preview_limit() -> usize {
    50
}

pub async fn preview(
    State(state): State<Arc<AppState>>,
    Path((venue, data_type, version)): Path<(String, String, String)>,
    Query(params): Query<PreviewParams>,
) -> ApiResult<Json<Value>> {
    let root = state
        .cfg
        .dataset_root
        .join(&venue)
        .join(&data_type)
        .join(&version);
    if !root.exists() {
        return Err(ApiError::NotFound(format!(
            "{venue}/{data_type}/{version}"
        )));
    }
    let glob = format!("{}/**/*.parquet", root.to_string_lossy());
    let sql = format!(
        "SELECT * FROM read_parquet('{glob}', hive_partitioning = true) ORDER BY _fetched_at DESC NULLS LAST LIMIT {}",
        params.limit
    );
    let duck = state.duck.lock().await;
    let result = query_engine::run(&duck.conn, &sql, params.limit)?;
    Ok(Json(serde_json::to_value(result)?))
}

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub sql: String,
    #[serde(default = "default_query_limit")]
    pub limit: usize,
}
fn default_query_limit() -> usize {
    10_000
}

pub async fn query(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QueryBody>,
) -> ApiResult<Json<Value>> {
    if body.sql.trim().is_empty() {
        return Err(ApiError::BadRequest("empty sql".into()));
    }
    let duck = state.duck.lock().await;
    let result = query_engine::run(&duck.conn, &body.sql, body.limit.max(1))?;
    Ok(Json(serde_json::to_value(result)?))
}

#[derive(Debug, Deserialize)]
pub struct ExportBody {
    pub sql: String,
    pub format: exporter::ExportFormat,
    /// Filename hint (without extension). Defaults to "scryer-export".
    pub name: Option<String>,
    /// Row cap for XLSX (CSV/Parquet stream the full result via DuckDB COPY).
    /// Default 1_000_000 — the Excel hard limit.
    #[serde(default = "default_xlsx_cap")]
    pub xlsx_row_cap: usize,
}
fn default_xlsx_cap() -> usize {
    1_000_000
}

pub async fn export(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ExportBody>,
) -> Result<Response, ApiError> {
    if body.sql.trim().is_empty() {
        return Err(ApiError::BadRequest("empty sql".into()));
    }
    let duck = state.duck.lock().await;
    let bytes = exporter::export(&duck.conn, &body.sql, body.format, body.xlsx_row_cap)?;
    let name = body.name.as_deref().unwrap_or("scryer-export");
    let filename = format!("{name}.{}", body.format.extension());
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, body.format.content_type())
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .body(Body::from(bytes))
        .map_err(|e| ApiError::Anyhow(e.into()))?;
    Ok(resp)
}
