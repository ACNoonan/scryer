//! `scryer-fetch-deribit` — Deribit public-API client for the DVOL
//! volatility index.
//!
//! Endpoint: `GET https://www.deribit.com/api/v2/public/get_volatility_index_data
//! ?currency=BTC&start_timestamp=ms&end_timestamp=ms&resolution=86400`
//!
//! Public, no auth. Response shape:
//! ```text
//! {"jsonrpc":"2.0","result":{
//!   "data":[[ts_ms, open, high, low, close], ...],
//!   "continuation":null
//! },"usIn":...}
//! ```
//!
//! `resolution` is in seconds; daily = 86400. Each row is OHLC; we
//! store only the `close` value as `dvol` per the locked v1 schema —
//! daily-close is the canonical "today's reading," and consumers
//! that need intraday OHLC can re-fetch with a finer resolution.

use std::time::Duration;

use scryer_schema::deribit_iv::v1::{DvolBar, SCHEMA_VERSION};
use scryer_schema::Meta;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://www.deribit.com";
pub const SOURCE_LABEL: &str = "deribit:get_volatility_index_data";
/// Daily resolution: 86400 seconds.
pub const RESOLUTION_DAILY_SECS: u64 = 86_400;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("upstream error envelope: {0}")]
    UpstreamError(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub base_url: String,
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    pub rate_limit_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            source_label: SOURCE_LABEL.to_string(),
            user_agent: concat!("scryer-fetch-deribit/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(250),
        }
    }
}

/// Fetch DVOL bars for `currency` (`"BTC"` or `"ETH"`) over
/// `[start_unix_secs, end_unix_secs]` at `resolution_secs` cadence.
pub async fn fetch_dvol(
    client: &reqwest::Client,
    cfg: &PollConfig,
    currency: &str,
    start_unix_secs: i64,
    end_unix_secs: i64,
    resolution_secs: u64,
    fetched_at: i64,
) -> Result<Vec<DvolBar>, FetchError> {
    let url = format!(
        "{}/api/v2/public/get_volatility_index_data",
        cfg.base_url.trim_end_matches('/')
    );
    let start_ms = (start_unix_secs * 1000).to_string();
    let end_ms = (end_unix_secs * 1000).to_string();
    let resolution = resolution_secs.to_string();
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("currency", currency),
                ("start_timestamp", start_ms.as_str()),
                ("end_timestamp", end_ms.as_str()),
                ("resolution", resolution.as_str()),
            ])
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            tracing::warn!(currency, status, "deribit transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        return parse_response(&text, currency, &cfg.source_label, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("deribit retries exhausted for {currency}"))
    }))
}

/// Parse the Deribit DVOL JSON body. Public for unit tests.
pub fn parse_response(
    body: &str,
    currency: &str,
    source_label: &str,
    fetched_at: i64,
) -> Result<Vec<DvolBar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if let Some(err) = v.get("error") {
        let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or("(no msg)");
        return Err(FetchError::UpstreamError(format!(
            "deribit error: {msg} (currency={currency})"
        )));
    }
    let arr = v
        .get("result")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<DvolBar> = Vec::with_capacity(arr.len());
    for entry in arr {
        // Each entry is [ts_ms, open, high, low, close].
        let tuple = match entry.as_array() {
            Some(t) if t.len() >= 5 => t,
            _ => continue,
        };
        let ts_ms = match tuple[0].as_i64() {
            Some(t) => t,
            None => continue,
        };
        let close = match tuple[4].as_f64() {
            Some(c) => c,
            None => continue,
        };
        out.push(DvolBar {
            underlying: currency.to_string(),
            ts: ts_ms / 1000,
            dvol: close,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, source_label),
        });
    }
    Ok(out)
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_response() {
        let body = r#"{"jsonrpc":"2.0","result":{
            "data":[[1776816000000,42.13,43.47,41.87,43.18],
                    [1776902400000,43.18,43.9,42.07,42.83]],
            "continuation":null
        }}"#;
        let rows = parse_response(body, "BTC", "deribit:test", 1_777_400_100).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].underlying, "BTC");
        assert_eq!(rows[0].ts, 1_776_816_000);
        assert_eq!(rows[0].dvol, 43.18);
        assert_eq!(rows[1].dvol, 42.83);
        assert_eq!(rows[0].meta.source, "deribit:test");
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"jsonrpc":"2.0","error":{"code":11050,"message":"bad currency"}}"#;
        let err = parse_response(body, "BTC", "deribit:test", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn missing_data_field_returns_zero_rows() {
        let body = r#"{"jsonrpc":"2.0","result":{"continuation":null}}"#;
        let rows = parse_response(body, "BTC", "deribit:test", 1).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn skips_truncated_tuples() {
        let body = r#"{"jsonrpc":"2.0","result":{"data":[
            [1776816000000,42.13],
            [1776902400000,43.18,43.9,42.07,42.83]
        ]}}"#;
        let rows = parse_response(body, "BTC", "deribit:test", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, 1_776_902_400);
    }
}
