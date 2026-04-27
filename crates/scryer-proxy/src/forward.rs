//! Outbound JSON-RPC forwarder.
//!
//! Builds a tuned reqwest client and sends one JSON-RPC request to one
//! upstream provider, returning `(status, body)` for the caller to
//! classify. Retry / provider selection happens above this layer.

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, USER_AGENT};
use serde_json::Value;

use crate::registry::ProviderState;

#[derive(Clone, Copy, Debug)]
pub struct ForwardConfig {
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub tcp_keepalive: Duration,
    pub pool_max_idle_per_host: usize,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            tcp_keepalive: Duration::from_secs(30),
            pool_max_idle_per_host: 10,
        }
    }
}

pub fn build_client(cfg: ForwardConfig) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .connect_timeout(cfg.connect_timeout)
        .tcp_keepalive(Some(cfg.tcp_keepalive))
        .pool_max_idle_per_host(cfg.pool_max_idle_per_host)
        .user_agent(concat!("scryer-proxy/", env!("CARGO_PKG_VERSION")))
        .build()
}

#[derive(Debug)]
pub enum ForwardError {
    BuildHeader { name: String },
    Transport(reqwest::Error),
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuildHeader { name } => write!(f, "invalid header `{name}` in provider config"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for ForwardError {}

pub struct ForwardResponse {
    pub status: u16,
    pub body: String,
    pub latency_ms: u32,
}

pub async fn forward(
    client: &reqwest::Client,
    provider: &ProviderState,
    payload: &Value,
) -> Result<ForwardResponse, ForwardError> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(concat!("scryer-proxy/", env!("CARGO_PKG_VERSION"))),
    );
    for h in &provider.config.headers {
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|_| ForwardError::BuildHeader { name: h.name.clone() })?;
        let value = HeaderValue::from_str(&h.value)
            .map_err(|_| ForwardError::BuildHeader { name: h.name.clone() })?;
        headers.insert(name, value);
    }

    let started = tokio::time::Instant::now();
    let resp = client
        .post(&provider.config.url)
        .headers(headers)
        .json(payload)
        .send()
        .await
        .map_err(ForwardError::Transport)?;
    let status = resp.status().as_u16();
    let body = resp.text().await.map_err(ForwardError::Transport)?;
    let latency_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
    Ok(ForwardResponse {
        status,
        body,
        latency_ms,
    })
}
