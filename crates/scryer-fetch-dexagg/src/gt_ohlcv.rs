//! GeckoTerminal historical OHLCV bars.
//!
//! Endpoint: `GET https://api.geckoterminal.com/api/v2/networks/{net}
//! /pools/{pool}/ohlcv/{timeframe}`
//!
//! Public REST, no auth, free-tier returns up to 100-182 daily bars
//! per pool per request. The `before_timestamp` cursor is paid-only
//! (verified 2026-04-26 — see wishlist item 41), so this is a
//! forward-accumulating tape rather than a backfill walker.
//!
//! Response shape:
//! ```text
//! {"data":{
//!   "id":"...","type":"ohlcv_request_response",
//!   "attributes":{
//!     "ohlcv_list":[[ts_seconds, open, high, low, close, volume_usd], ...]
//!   }
//! }}
//! ```

use std::time::Duration;

use scryer_schema::geckoterminal_ohlcv::v1::{Bar, SCHEMA_VERSION};
use scryer_schema::Meta;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://api.geckoterminal.com/api/v2";
pub const SOURCE_LABEL: &str = "geckoterminal:ohlcv";
pub const DEFAULT_NETWORK: &str = "solana";

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub base_url: String,
    pub network: String,
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            network: DEFAULT_NETWORK.to_string(),
            source_label: SOURCE_LABEL.to_string(),
            user_agent: concat!("scryer-fetch-dexagg/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Fetch the most-recent OHLCV bars for `pool` at `timeframe`. Free-
/// tier returns up to 100-182 bars per call.
pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pool: &str,
    timeframe: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/networks/{}/pools/{}/ohlcv/{}",
        cfg.base_url.trim_end_matches('/'),
        cfg.network,
        pool,
        timeframe
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .header("Accept", "application/json")
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
            tracing::warn!(pool, status, "geckoterminal ohlcv transient error; backing off");
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
        return parse_response(&text, pool, timeframe, &cfg.source_label, fetched_at);
    }
    Err(last_err.unwrap_or(FetchError::MalformedBody(format!(
        "retries exhausted for pool={pool}"
    ))))
}

/// Parse the GeckoTerminal OHLCV JSON body. Public for tests.
pub fn parse_response(
    body: &str,
    pool: &str,
    timeframe: &str,
    source_label: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let arr = v
        .get("data")
        .and_then(|d| d.get("attributes"))
        .and_then(|a| a.get("ohlcv_list"))
        .and_then(|l| l.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Bar> = Vec::with_capacity(arr.len());
    for entry in arr {
        let tup = match entry.as_array() {
            Some(t) if t.len() >= 6 => t,
            _ => continue,
        };
        let ts = match tup[0].as_i64() {
            Some(t) => t,
            None => continue,
        };
        let open = match tup[1].as_f64() {
            Some(v) => v,
            None => continue,
        };
        let high = match tup[2].as_f64() {
            Some(v) => v,
            None => continue,
        };
        let low = match tup[3].as_f64() {
            Some(v) => v,
            None => continue,
        };
        let close = match tup[4].as_f64() {
            Some(v) => v,
            None => continue,
        };
        let volume = tup[5].as_f64().unwrap_or(0.0);
        let dt = (ts / 86_400) as i32;
        out.push(Bar {
            pool_address: pool.to_string(),
            timeframe: timeframe.to_string(),
            ts,
            dt,
            open,
            high,
            low,
            close,
            volume_usd: volume,
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
        let body = r#"{"data":{"id":"x","type":"ohlcv_request_response","attributes":{
            "ohlcv_list":[
                [1777420800,83.99,84.20,83.70,83.88,11015.24],
                [1777334400,82.50,84.10,82.30,83.99,15000.12]
            ]
        }}}"#;
        let rows = parse_response(body, "POOL1", "day", "geckoterminal:ohlcv", 1_777_400_100)
            .expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].pool_address, "POOL1");
        assert_eq!(rows[0].timeframe, "day");
        assert_eq!(rows[0].ts, 1_777_420_800);
        assert!((rows[0].close - 83.88).abs() < 1e-9);
        assert!((rows[0].volume_usd - 11_015.24).abs() < 1e-9);
        // dt = ts / 86400
        assert_eq!(rows[0].dt, 1_777_420_800 / 86_400);
    }

    #[test]
    fn missing_ohlcv_list_returns_zero_rows() {
        let body = r#"{"data":{"attributes":{}}}"#;
        let rows = parse_response(body, "POOL1", "day", "x", 1).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn skips_truncated_tuples() {
        let body = r#"{"data":{"attributes":{"ohlcv_list":[
            [1,2,3],
            [1777420800,83.99,84.20,83.70,83.88,11015.24]
        ]}}}"#;
        let rows = parse_response(body, "POOL1", "day", "x", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, 1_777_420_800);
    }

    #[test]
    fn rejects_malformed_body() {
        let err = parse_response("{not json", "P", "day", "x", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
