//! Hyperliquid funding-history client.
//!
//! Endpoint: `POST https://api.hyperliquid.xyz/info`
//! with body `{"type":"fundingHistory","coin":"BTC","startTime":...,"endTime":...}`.
//!
//! Public, no auth. Funding cadence is 1 hour. Hyperliquid's `coin` is
//! the canonical short ticker (`"BTC"`, `"ETH"`, `"SOL"`), no quote
//! suffix — they're all USD-margined perps.
//!
//! Response is a flat array; each element looks like:
//! ```json
//! {
//!   "coin": "BTC",
//!   "fundingRate": "0.0000125",
//!   "premium": "0.0000098",
//!   "time": 1777392000000
//! }
//! ```
//!
//! Note: `time` is ms since epoch as a number (NOT a string, unlike
//! OKX). Hyperliquid does not surface mark_price on this endpoint, so
//! we leave it `None` in the row.

use scryer_schema::cex_perp_funding_multi::v1::{Rate, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.hyperliquid.xyz";
pub const SOURCE_LABEL: &str = "hyperliquid:fundingHistory";
/// Hyperliquid funding cadence: 1 hour.
pub const FUNDING_PERIOD_SECS: i32 = 3600;

/// Fetch funding observations for `coin` in `[start_ms, end_ms)`.
///
/// `coin` is the Hyperliquid short ticker, e.g. `"BTC"`. Hyperliquid
/// caps the response at 500 records per call; if the window is wider
/// than ~21 days at 1h cadence, paginate by raising `start_ms`.
pub async fn fetch_funding(
    client: &reqwest::Client,
    cfg: &PollConfig,
    coin: &str,
    start_ms: i64,
    end_ms: Option<i64>,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let url = format!("{}/info", DEFAULT_BASE_URL.trim_end_matches('/'));
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let mut body = serde_json::json!({
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
        });
        if let Some(end) = end_ms {
            body["endTime"] = serde_json::Value::from(end);
        }
        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
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
            tracing::warn!(coin, status, "hyperliquid transient error; backing off");
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
        return parse_response(&text, coin, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("hyperliquid retries exhausted for {coin}"))
    }))
}

/// Parse the Hyperliquid `fundingHistory` JSON body into [`Rate`] rows.
/// Public for unit tests.
pub fn parse_response(
    body: &str,
    coin: &str,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    // Hyperliquid surfaces errors as a top-level object with an
    // `"error"` key instead of an array.
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Err(FetchError::UpstreamError(format!(
            "hyperliquid error: {err}"
        )));
    }
    let arr = match v.as_array() {
        Some(a) => a,
        None => {
            return Err(FetchError::MalformedBody(format!(
                "expected top-level array, got: {}",
                body_head(body)
            )));
        }
    };
    let mut out: Vec<Rate> = Vec::with_capacity(arr.len());
    for entry in arr {
        let rate = entry
            .get("fundingRate")
            .and_then(|r| r.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let time_ms = entry.get("time").and_then(|t| t.as_i64());
        let (rate, time_ms) = match (rate, time_ms) {
            (Some(r), Some(t)) => (r, t),
            _ => continue,
        };
        out.push(Rate {
            exchange: "hyperliquid".to_string(),
            symbol: coin.to_string(),
            exchange_symbol: coin.to_string(),
            funding_ts: time_ms / 1000,
            funding_rate: rate,
            mark_price: None,
            funding_period_secs: FUNDING_PERIOD_SECS,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_response() {
        let body = r#"[
            {"coin":"BTC","fundingRate":"0.0000125","premium":"0.0000098","time":1777392000000},
            {"coin":"BTC","fundingRate":"0.0000131","premium":"0.0000110","time":1777395600000}
        ]"#;
        let rows = parse_response(body, "BTC", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "hyperliquid");
        assert_eq!(rows[0].symbol, "BTC");
        assert_eq!(rows[0].exchange_symbol, "BTC");
        assert_eq!(rows[0].funding_ts, 1_777_392_000);
        assert_eq!(rows[0].funding_rate, 0.0000125);
        assert_eq!(rows[0].mark_price, None);
        assert_eq!(rows[0].funding_period_secs, FUNDING_PERIOD_SECS);
        assert_eq!(rows[0].meta.source, SOURCE_LABEL);
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"error":"invalid coin"}"#;
        let err = parse_response(body, "BTC", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn rejects_non_array_body() {
        let body = r#"{"foo": "bar"}"#;
        let err = parse_response(body, "BTC", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn skips_rows_with_unparseable_rate() {
        let body = r#"[
            {"coin":"BTC","fundingRate":"oops","time":1},
            {"coin":"BTC","fundingRate":"0.0001","time":2000}
        ]"#;
        let rows = parse_response(body, "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].funding_ts, 2);
    }
}
