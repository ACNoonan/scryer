//! Coinbase International Exchange funding-rate client.
//!
//! Endpoint: `GET https://api.international.coinbase.com/api/v1/instruments/{symbol}/funding`
//!
//! Public, no auth. Symbol format is `{BASE}-PERP`, e.g. `BTC-PERP`,
//! `ETH-PERP`, `SOL-PERP`. Funding cadence is 1 hour.
//!
//! Response shape:
//! ```json
//! {
//!   "pagination": {"result_limit": 100, "result_offset": 0},
//!   "results": [
//!     {
//!       "instrument_id": "149264167780483072",
//!       "funding_rate": "0.000005321",
//!       "mark_price": "85123.45",
//!       "event_time": "2026-04-28T15:00:00Z"
//!     },
//!     ...
//!   ]
//! }
//! ```
//!
//! `event_time` is RFC3339 UTC. We parse to unix seconds without
//! pulling in chrono — the format is fixed and length-deterministic, so
//! a minimal hand parser is fine.

use scryer_schema::cex_perp_funding_multi::v1::{Rate, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.international.coinbase.com";
pub const SOURCE_LABEL: &str = "coinbase_intl:instrument-funding";
/// Coinbase International funding cadence: 1 hour.
pub const FUNDING_PERIOD_SECS: i32 = 3600;

/// Fetch up to `limit` funding observations for `symbol`.
///
/// `symbol` is the venue-specific instrument id, e.g. `"BTC-PERP"`.
/// `canonical_symbol` is the short symbol carried into the row, e.g.
/// `"BTC"`. `limit` defaults to 100 upstream; the API caps at 100 per
/// request and supports `result_offset` for pagination, but the
/// default-100 window is sufficient for hourly polling.
pub async fn fetch_funding(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
    canonical_symbol: &str,
    limit: u32,
    offset: u32,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let url = format!(
        "{}/api/v1/instruments/{}/funding",
        DEFAULT_BASE_URL.trim_end_matches('/'),
        symbol
    );
    let limit_str = limit.to_string();
    let offset_str = offset.to_string();
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("result_limit", limit_str.as_str()),
                ("result_offset", offset_str.as_str()),
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
            tracing::warn!(symbol, status, "coinbase_intl transient error; backing off");
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
        return parse_response(&text, symbol, canonical_symbol, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("coinbase_intl retries exhausted for {symbol}"))
    }))
}

/// Parse the Coinbase International funding-history JSON body into
/// [`Rate`] rows. Public for unit tests.
pub fn parse_response(
    body: &str,
    symbol: &str,
    canonical_symbol: &str,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if let Some(msg) = v.get("message").and_then(|m| m.as_str()) {
        if !msg.is_empty() {
            return Err(FetchError::UpstreamError(format!(
                "coinbase_intl message: {msg}"
            )));
        }
    }
    let results = v
        .get("results")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Rate> = Vec::with_capacity(results.len());
    for entry in results {
        let rate = entry
            .get("funding_rate")
            .and_then(|r| r.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let event_time = entry.get("event_time").and_then(|t| t.as_str());
        let (rate, event_time) = match (rate, event_time) {
            (Some(r), Some(t)) => (r, t),
            _ => continue,
        };
        let funding_ts = match parse_rfc3339_to_unix(event_time) {
            Some(t) => t,
            None => continue,
        };
        let mark_price = entry
            .get("mark_price")
            .and_then(|m| m.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        out.push(Rate {
            exchange: "coinbase_intl".to_string(),
            symbol: canonical_symbol.to_string(),
            exchange_symbol: symbol.to_string(),
            funding_ts,
            funding_rate: rate,
            mark_price,
            funding_period_secs: FUNDING_PERIOD_SECS,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fff]Z` -> unix seconds. Returns `None`
/// on any deviation. The Coinbase International API consistently emits
/// this exact format, so we don't try to be clever.
fn parse_rfc3339_to_unix(s: &str) -> Option<i64> {
    // Required minimum: "2026-04-28T15:00:00Z" = 20 chars.
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
    // Civil-from-Y/M/D using Howard Hinnant's algorithm (works for any
    // proleptic Gregorian date, no leap-year edge cases to worry about
    // for the tracker timeframe of interest).
    Some(days_from_civil(year, month, day) * 86_400
        + (hour as i64) * 3_600
        + (minute as i64) * 60
        + (second as i64))
}

/// Days since unix epoch (1970-01-01) for a proleptic-Gregorian date.
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
            "pagination": {"result_limit": 100, "result_offset": 0},
            "results": [
                {"instrument_id":"149264167780483072","funding_rate":"0.000005321","mark_price":"85123.45","event_time":"2026-04-28T15:00:00Z"},
                {"instrument_id":"149264167780483072","funding_rate":"0.000004102","mark_price":"85100.10","event_time":"2026-04-28T14:00:00Z"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-PERP", "BTC", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "coinbase_intl");
        assert_eq!(rows[0].symbol, "BTC");
        assert_eq!(rows[0].exchange_symbol, "BTC-PERP");
        assert_eq!(rows[0].funding_rate, 0.000005321);
        assert_eq!(rows[0].mark_price, Some(85_123.45));
        assert_eq!(rows[0].funding_period_secs, FUNDING_PERIOD_SECS);
    }

    #[test]
    fn rfc3339_parser_matches_known_anchor() {
        // 2026-04-28T15:00:00Z -> unix
        // Sanity: 2026-01-01 = 56 yrs after 1970 = roughly
        // 1_767_225_600. Computing precisely is what the parser is
        // for, so just check round-trip-ish properties:
        let a = parse_rfc3339_to_unix("2026-04-28T15:00:00Z").expect("ok");
        let b = parse_rfc3339_to_unix("2026-04-28T16:00:00Z").expect("ok");
        assert_eq!(b - a, 3600);
    }

    #[test]
    fn rfc3339_parser_rejects_bad_format() {
        assert!(parse_rfc3339_to_unix("not a date").is_none());
        assert!(parse_rfc3339_to_unix("2026/04/28T15:00:00Z").is_none());
        assert!(parse_rfc3339_to_unix("2026-04-28").is_none());
    }

    #[test]
    fn rfc3339_anchors_match_unix_epoch() {
        assert_eq!(parse_rfc3339_to_unix("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_to_unix("2000-01-01T00:00:00Z"),
            Some(946_684_800)
        );
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"message":"instrument not found"}"#;
        let err = parse_response(body, "BTC-PERP", "BTC", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn skips_rows_with_missing_fields() {
        let body = r#"{
            "results": [
                {"funding_rate":"0.0001","event_time":"2026-04-28T15:00:00Z"},
                {"funding_rate":"oops","event_time":"2026-04-28T14:00:00Z"},
                {"funding_rate":"0.0002"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-PERP", "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].mark_price, None);
    }
}
