//! `scryer-proxy` — JSON-RPC proxy library.
//!
//! Pattern-lifted from relay-sol; v0.1 ships only the subset listed
//! under "Proxy crate v0.1 scope" in `methodology_log.md`. Major
//! deferred features (WS fan-out, dashboard, OTel, doctor, replay,
//! cloud secrets, SQLite cache, hot-reload, anomaly z-score, hedging,
//! tier weighting, commitment-aware routing) are explicitly out of
//! scope until later phases.
//!
//! Public entry points:
//!
//! - [`build_router`] — produce an `axum::Router` ready for serving.
//! - [`spawn_health_loop`] — start the background probe task.
//! - [`SolanaChain`] — the v0.1 chain config (see [`chain`] for the
//!   trait if you need to add a new chain).

pub mod chain;
pub mod error;
pub mod forward;
pub mod health;
pub mod metrics;
pub mod quota;
pub mod registry;
pub mod router;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use prometheus::Encoder;

pub use chain::{ChainConfig, SolanaChain};
pub use error::{InitError, ProxyError};
pub use forward::ForwardConfig;
pub use health::HealthConfig;
pub use metrics::Metrics;
pub use registry::{ProviderConfig, ProviderState, Registry};
pub use router::RetryConfig;

pub struct ProxyState {
    pub registry: Arc<Registry>,
    pub chain: Arc<dyn ChainConfig>,
    pub client: reqwest::Client,
    pub metrics: Arc<Metrics>,
    pub retry: RetryConfig,
}

/// Build the axum router for a fully-wired proxy. The caller is
/// responsible for binding it to a `tokio::net::TcpListener`.
pub fn build_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/rpc", post(router::handle_jsonrpc))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

async fn healthz(
    axum::extract::State(state): axum::extract::State<Arc<ProxyState>>,
) -> axum::http::StatusCode {
    if state
        .registry
        .providers
        .iter()
        .any(|p| p.is_healthy() && !p.is_quarantined())
    {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<Arc<ProxyState>>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let encoder = prometheus::TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&state.metrics.registry.gather(), &mut buf)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let body = String::from_utf8(buf).map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response())
}

use axum::response::IntoResponse;

/// Spawn the background health-probe loop. Returns the join handle so
/// integration tests can cancel it deterministically.
pub fn spawn_health_loop(state: Arc<ProxyState>, cfg: HealthConfig) -> tokio::task::JoinHandle<()> {
    health::spawn_loop(
        state.registry.clone(),
        state.chain.clone(),
        state.client.clone(),
        state.metrics.clone(),
        cfg,
    )
}
