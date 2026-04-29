//! `scryer-fetch-xstocks` — Backed Finance xStocks public-API client.
//!
//! Endpoints used (`https://api.xstocks.fi/api/v2/public/*`,
//! no auth, 1000 req/min rate-limit):
//!
//! - `GET assets/{symbol}/price-data`
//!     Returns `{"quote": <number|null>}` — Backed's continuously-
//!     updating indicative quote ("fair value"). The response
//!     carries no timestamp; the operator stamps `_fetched_at`.
//! - `GET assets/{symbol}/multiplier?network=Solana`
//!     Returns `{"currentMultiplier": <f64>, "newMultiplier": ...,
//!     "activationDateTime": ..., "reason": ...}` — captures
//!     dividend / split adjustments cumulative across listing date.
//! - `GET system/status/{symbol}`
//!     Returns `{"isMarketTradingHalted": bool, "isAtomicTradingHalted": bool}`
//!     — when either is true, the quote is meaningless / stale.
//!
//! All three combine into one `backed_nav_strikes.v1::Strike` row
//! per (symbol, fetched_at). Tolerant per-call: a per-symbol
//! enrichment failure doesn't fail the whole row — only price-data
//! is required.

use std::time::Duration;

use scryer_schema::backed_nav_strikes::v1::{Strike, SCHEMA_VERSION};
use scryer_schema::Meta;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://api.xstocks.fi";
pub const SOURCE_LABEL: &str = "xstocks_api_v2";

/// Default xStock symbol set used by scry's CLI. Mirrors the
/// canonical 8-symbol registry in scryer-fetch-dexagg::jupiter
/// (xStocks list curated by Backed's `cowswap-xstocks-tokenlist`
/// repo). Add new symbols here as Backed lists them.
pub const DEFAULT_SYMBOLS: &[&str] = &[
    "SPYx", "QQQx", "TSLAx", "GOOGLx", "AAPLx", "NVDAx", "MSTRx", "HOODx",
];

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
    /// Inter-call delay (per HTTP request). 1000/min upstream cap
    /// → 60ms/req minimum; 100ms default leaves headroom.
    pub rate_limit_delay: Duration,
    /// Network for the multiplier endpoint. `Solana` is the
    /// scryer-canonical chain; xStocks also live on Ethereum,
    /// Arbitrum, Base, BinanceSmartChain, Ton, HyperEVM.
    pub network: String,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            source_label: SOURCE_LABEL.to_string(),
            user_agent: concat!("scryer-fetch-xstocks/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(15),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(100),
            network: "Solana".to_string(),
        }
    }
}

/// Build a reusable [`reqwest::Client`].
pub fn build_client(cfg: &PollConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(cfg.user_agent.clone())
        .timeout(cfg.request_timeout)
        .build()
}

/// Fetch one [`Strike`] for `token_symbol`. Issues 3 calls
/// (price-data, multiplier, system/status); enrichment failures
/// degrade the row gracefully (multiplier/halt fields go null).
/// Returns `None` if the price-data call returns null quote.
pub async fn fetch_strike(
    client: &reqwest::Client,
    cfg: &PollConfig,
    token_symbol: &str,
    fetched_at: i64,
) -> Result<Option<Strike>, FetchError> {
    let nav_value = match fetch_price_data(client, cfg, token_symbol).await? {
        Some(q) => q,
        None => return Ok(None),
    };
    if cfg.rate_limit_delay > Duration::ZERO {
        tokio::time::sleep(cfg.rate_limit_delay).await;
    }
    let current_multiplier = match fetch_multiplier(client, cfg, token_symbol).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(symbol = %token_symbol, error = %e, "multiplier fetch skipped");
            None
        }
    };
    if cfg.rate_limit_delay > Duration::ZERO {
        tokio::time::sleep(cfg.rate_limit_delay).await;
    }
    let (is_market_halted, is_atomic_halted) =
        match fetch_system_status(client, cfg, token_symbol).await {
            Ok((m, a)) => (m, a),
            Err(e) => {
                tracing::warn!(symbol = %token_symbol, error = %e, "system status fetch skipped");
                (None, None)
            }
        };

    Ok(Some(Strike {
        token_symbol: token_symbol.to_string(),
        nav_ts: fetched_at,
        nav_value,
        current_multiplier,
        is_market_halted,
        is_atomic_halted,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, &cfg.source_label),
    }))
}

/// `GET /api/v2/public/assets/{symbol}/price-data` →
/// `Option<f64>` (None when quote is null upstream).
pub async fn fetch_price_data(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
) -> Result<Option<f64>, FetchError> {
    let url = format!(
        "{}/api/v2/public/assets/{}/price-data",
        cfg.base_url.trim_end_matches('/'),
        symbol
    );
    let text = http_get_with_retry(client, cfg, &url, &[]).await?;
    parse_price_data_response(&text)
}

pub fn parse_price_data_response(body: &str) -> Result<Option<f64>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let q = v.get("quote");
    match q {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(x) => x
            .as_f64()
            .ok_or_else(|| {
                FetchError::MalformedBody(format!("quote not numeric: {x}"))
            })
            .map(Some),
    }
}

/// `GET /api/v2/public/assets/{symbol}/multiplier?network={network}`
/// → `Option<f64>` (currentMultiplier; None if upstream omits).
pub async fn fetch_multiplier(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
) -> Result<Option<f64>, FetchError> {
    let url = format!(
        "{}/api/v2/public/assets/{}/multiplier",
        cfg.base_url.trim_end_matches('/'),
        symbol
    );
    let text = http_get_with_retry(client, cfg, &url, &[("network", cfg.network.as_str())]).await?;
    parse_multiplier_response(&text)
}

pub fn parse_multiplier_response(body: &str) -> Result<Option<f64>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    Ok(v.get("currentMultiplier").and_then(|x| x.as_f64()))
}

/// `GET /api/v2/public/system/status/{symbol}` →
/// `(is_market_halted, is_atomic_halted)`.
pub async fn fetch_system_status(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
) -> Result<(Option<bool>, Option<bool>), FetchError> {
    let url = format!(
        "{}/api/v2/public/system/status/{}",
        cfg.base_url.trim_end_matches('/'),
        symbol
    );
    let text = http_get_with_retry(client, cfg, &url, &[]).await?;
    parse_system_status_response(&text)
}

pub fn parse_system_status_response(
    body: &str,
) -> Result<(Option<bool>, Option<bool>), FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let mh = v.get("isMarketTradingHalted").and_then(|x| x.as_bool());
    let ah = v.get("isAtomicTradingHalted").and_then(|x| x.as_bool());
    Ok((mh, ah))
}

async fn http_get_with_retry(
    client: &reqwest::Client,
    cfg: &PollConfig,
    url: &str,
    query: &[(&str, &str)],
) -> Result<String, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let mut req = client.get(url);
        if !query.is_empty() {
            req = req.query(query);
        }
        let resp = req.send().await;
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
            tracing::warn!(url, status, "xstocks transient error; backing off");
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
        return Ok(text);
    }
    Err(last_err.unwrap_or_else(|| FetchError::UpstreamError("retries exhausted".to_string())))
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_price_data() {
        assert_eq!(
            parse_price_data_response(r#"{"quote":710.48}"#).unwrap(),
            Some(710.48)
        );
        assert_eq!(
            parse_price_data_response(r#"{"quote":269.085}"#).unwrap(),
            Some(269.085)
        );
    }

    #[test]
    fn null_quote_is_none() {
        assert_eq!(
            parse_price_data_response(r#"{"quote":null}"#).unwrap(),
            None
        );
        assert_eq!(parse_price_data_response(r#"{}"#).unwrap(), None);
    }

    #[test]
    fn rejects_non_numeric_quote() {
        let err = parse_price_data_response(r#"{"quote":"700"}"#).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn parses_multiplier_response() {
        let body = r#"{"currentMultiplier":1.0025607582229898,"newMultiplier":0,"activationDateTime":0,"reason":"Dividend"}"#;
        assert_eq!(
            parse_multiplier_response(body).unwrap(),
            Some(1.0025607582229898)
        );
    }

    #[test]
    fn parses_system_status() {
        let body = r#"{"isMarketTradingHalted":false,"isAtomicTradingHalted":true}"#;
        let (mh, ah) = parse_system_status_response(body).unwrap();
        assert_eq!(mh, Some(false));
        assert_eq!(ah, Some(true));
    }

    #[test]
    fn missing_status_fields_return_none() {
        let body = r#"{}"#;
        let (mh, ah) = parse_system_status_response(body).unwrap();
        assert_eq!(mh, None);
        assert_eq!(ah, None);
    }
}
