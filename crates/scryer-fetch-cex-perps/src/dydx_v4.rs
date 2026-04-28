//! dYdX v4 historical-funding client.
//!
//! Endpoint: `GET https://indexer.dydx.trade/v4/historicalFunding/{ticker}`
//!
//! Public, no auth. `ticker` is `{BASE}-USD`, e.g. `BTC-USD`,
//! `ETH-USD`. dYdX v4 perps are USD-margined. Funding cadence is
//! 1 hour.
//!
//! Response shape:
//! ```json
//! {
//!   "historicalFunding": [
//!     {
//!       "ticker": "BTC-USD",
//!       "rate": "0.0000098",
//!       "price": "85123.45",
//!       "effectiveAt": "2026-04-28T15:00:00.000Z",
//!       "effectiveAtHeight": "27123456"
//!     },
//!     ...
//!   ]
//! }
//! ```
//!
//! `effectiveAt` is RFC3339 with millisecond precision; we drop the
//! sub-second part. `price` here is the index price observed at the
//! funding settlement, populated as `mark_price` in the row.

use scryer_schema::cex_perp_funding_multi::v1::{Rate, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://indexer.dydx.trade";
pub const SOURCE_LABEL: &str = "dydx_v4:historicalFunding";
/// dYdX v4 funding cadence: 1 hour.
pub const FUNDING_PERIOD_SECS: i32 = 3600;

/// Fetch up to 100 funding observations for `ticker`.
///
/// `ticker` is the dYdX market id, e.g. `"BTC-USD"`. `canonical_symbol`
/// is the short symbol carried into the row (`"BTC"`). dYdX caps each
/// response at 100 entries and supports `?effectiveBeforeOrAt=...`
/// (RFC3339) for paginating into older history.
pub async fn fetch_funding(
    client: &reqwest::Client,
    cfg: &PollConfig,
    ticker: &str,
    canonical_symbol: &str,
    effective_before_or_at: Option<&str>,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let url = format!(
        "{}/v4/historicalFunding/{}",
        DEFAULT_BASE_URL.trim_end_matches('/'),
        ticker
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let mut req = client.get(&url);
        if let Some(before) = effective_before_or_at {
            req = req.query(&[("effectiveBeforeOrAt", before)]);
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
            tracing::warn!(ticker, status, "dydx_v4 transient error; backing off");
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
        return parse_response(&text, ticker, canonical_symbol, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("dydx_v4 retries exhausted for {ticker}"))
    }))
}

/// Parse the dYdX v4 historical-funding JSON body into [`Rate`] rows.
/// Public for unit tests.
pub fn parse_response(
    body: &str,
    ticker: &str,
    canonical_symbol: &str,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    // dYdX surfaces errors as `{"errors":[{"msg":"..."}]}`.
    if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {
        if !errs.is_empty() {
            let msg = errs
                .first()
                .and_then(|e| e.get("msg"))
                .and_then(|m| m.as_str())
                .unwrap_or("(no msg)");
            return Err(FetchError::UpstreamError(format!(
                "dydx_v4 error: {msg}"
            )));
        }
    }
    let entries = v
        .get("historicalFunding")
        .and_then(|h| h.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Rate> = Vec::with_capacity(entries.len());
    for entry in entries {
        let rate = entry
            .get("rate")
            .and_then(|r| r.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let effective_at = entry.get("effectiveAt").and_then(|t| t.as_str());
        let (rate, effective_at) = match (rate, effective_at) {
            (Some(r), Some(t)) => (r, t),
            _ => continue,
        };
        let funding_ts = match parse_rfc3339_to_unix(effective_at) {
            Some(t) => t,
            None => continue,
        };
        let mark_price = entry
            .get("price")
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        out.push(Rate {
            exchange: "dydx_v4".to_string(),
            symbol: canonical_symbol.to_string(),
            exchange_symbol: ticker.to_string(),
            funding_ts,
            funding_rate: rate,
            mark_price,
            funding_period_secs: FUNDING_PERIOD_SECS,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fff]Z` -> unix seconds, dropping the
/// optional millisecond fraction. Returns `None` on any deviation.
fn parse_rfc3339_to_unix(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let year: i64 = std::str::from_utf8(&bytes[0..4]).ok()?.parse().ok()?;
    if bytes[4] != b'-' {
        return None;
    }
    let month: u32 = std::str::from_utf8(&bytes[5..7]).ok()?.parse().ok()?;
    if bytes[7] != b'-' {
        return None;
    }
    let day: u32 = std::str::from_utf8(&bytes[8..10]).ok()?.parse().ok()?;
    if bytes[10] != b'T' {
        return None;
    }
    let hour: u32 = std::str::from_utf8(&bytes[11..13]).ok()?.parse().ok()?;
    if bytes[13] != b':' {
        return None;
    }
    let minute: u32 = std::str::from_utf8(&bytes[14..16]).ok()?.parse().ok()?;
    if bytes[16] != b':' {
        return None;
    }
    let second: u32 = std::str::from_utf8(&bytes[17..19]).ok()?.parse().ok()?;
    Some(days_from_civil(year, month, day) * 86_400
        + (hour as i64) * 3_600
        + (minute as i64) * 60
        + (second as i64))
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_response() {
        let body = r#"{
            "historicalFunding": [
                {"ticker":"BTC-USD","rate":"0.0000098","price":"85123.45","effectiveAt":"2026-04-28T15:00:00.000Z","effectiveAtHeight":"27123456"},
                {"ticker":"BTC-USD","rate":"0.0000110","price":"85100.10","effectiveAt":"2026-04-28T14:00:00.000Z","effectiveAtHeight":"27123100"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USD", "BTC", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "dydx_v4");
        assert_eq!(rows[0].symbol, "BTC");
        assert_eq!(rows[0].exchange_symbol, "BTC-USD");
        assert_eq!(rows[0].funding_rate, 0.0000098);
        assert_eq!(rows[0].mark_price, Some(85_123.45));
        assert_eq!(rows[0].funding_period_secs, FUNDING_PERIOD_SECS);
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"errors":[{"msg":"market not found"}]}"#;
        let err = parse_response(body, "BTC-USD", "BTC", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn skips_rows_with_unparseable_fields() {
        let body = r#"{
            "historicalFunding": [
                {"ticker":"BTC-USD","rate":"oops","effectiveAt":"2026-04-28T15:00:00.000Z"},
                {"ticker":"BTC-USD","rate":"0.0001","effectiveAt":"bad-timestamp"},
                {"ticker":"BTC-USD","rate":"0.0002","effectiveAt":"2026-04-28T14:00:00.000Z"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USD", "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].funding_rate, 0.0002);
    }
}
