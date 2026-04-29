//! Yahoo Finance equity corporate-actions fetcher.
//!
//! Endpoint: `https://query2.finance.yahoo.com/v8/finance/chart/{symbol}
//! ?period1={start_unix}&period2={end_unix}&interval=1d&events=div|split`
//!
//! Returns JSON with `chart.result[0].events.dividends` and
//! `chart.result[0].events.splits` maps. yfinance's
//! `Ticker.actions` / `Ticker.splits` / `Ticker.dividends` are thin
//! wrappers over the same response. The fetcher decodes both maps
//! into a single flat [`Action`] row stream.
//!
//! # Why Yahoo and not Stooq / Finnhub
//!
//! Stooq's free CSV doesn't surface dividend/split events, and
//! Finnhub's free tier limits corp-action history to ~30 days. For
//! the soothsayer Paper-1 §10.2 follow-up (2023-01 → present per-
//! symbol corp-action panel for 10 underliers), Yahoo's chart
//! `events=div|split` query is the lowest-friction first-party
//! source. It generally works without the v10 quoteSummary crumb
//! handshake when accessed with a non-bot User-Agent — but if Yahoo
//! tightens the gate, the upstream surfaces 401/429 and the fetcher
//! returns [`FetchError::UpstreamStatus`] verbatim rather than
//! silently retrying. The Paper-1 backfill is a one-shot operation,
//! not a daily forward-tape, so the bot-detection treadmill that
//! drove the bars/earnings sources off Yahoo (see lib.rs) is far
//! less of an issue here.

use std::collections::BTreeSet;

use scryer_schema::yahoo_corp_actions::v1::{
    Action, EVENT_CASH_DIVIDEND, EVENT_SPLIT,
};
use scryer_schema::Meta;

use crate::{unix_seconds_to_date32, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://query2.finance.yahoo.com";

/// Fetch all dividend + split events for `symbol` whose timestamp falls
/// in `[start_unix, end_unix]` (inclusive). Yahoo's chart endpoint
/// returns the entire event history clipped to the requested window;
/// callers should pass the largest sensible window for a one-shot
/// backfill (e.g. `period1 = 0`, `period2 = now+1d`) and let the
/// caller's `--start` / `--end` clip on the output side.
///
/// Empty results — symbol with no events in window, ETF without
/// dividend stream, etc. — return `Ok(Vec::new())` rather than error.
pub async fn fetch_corp_actions(
    client: &reqwest::Client,
    cfg: &PollConfig,
    base_url: &str,
    symbol: &str,
    start_unix: i64,
    end_unix: i64,
    meta: &Meta,
) -> Result<Vec<Action>, FetchError> {
    let url = format!(
        "{}/v8/finance/chart/{}",
        base_url.trim_end_matches('/'),
        symbol
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("period1", start_unix.to_string()),
                ("period2", end_unix.to_string()),
                ("interval", "1d".to_string()),
                ("events", "div|split".to_string()),
                ("includePrePost", "false".to_string()),
            ])
            .timeout(cfg.request_timeout)
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
        if status == 401 || status == 403 {
            // Yahoo's bot-detection wall — don't retry, surface
            // verbatim so the operator sees the actual gate.
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        if status >= 400 {
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        return parse_response(&text, symbol, start_unix, end_unix, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("yahoo chart retries exhausted for {symbol}"))
    }))
}

/// Parse a Yahoo chart-events JSON response into [`Action`] rows.
/// Public so tests can drive it directly with hardcoded JSON.
///
/// Returned rows are clipped to events whose unix-timestamp is in
/// `[start_unix, end_unix]` and de-duplicated by
/// `(event_date, event_type)` (Yahoo occasionally returns near-
/// duplicate keys for the same event — keep the first).
pub fn parse_response(
    body: &str,
    symbol: &str,
    start_unix: i64,
    end_unix: i64,
    meta: &Meta,
) -> Result<Vec<Action>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let chart = v
        .get("chart")
        .ok_or_else(|| FetchError::MalformedBody("missing chart key".into()))?;
    if let Some(err) = chart.get("error").and_then(|e| {
        if e.is_null() {
            None
        } else {
            Some(e.clone())
        }
    }) {
        let desc = err
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or("(no description)");
        let code = err.get("code").and_then(|c| c.as_str()).unwrap_or("?");
        return Err(FetchError::UpstreamError(format!(
            "yahoo.chart.error: code={code} desc={desc} (symbol={symbol})"
        )));
    }
    let result = chart
        .get("result")
        .and_then(|r| r.as_array())
        .and_then(|r| r.first())
        .cloned();
    let result = match result {
        Some(r) => r,
        None => return Ok(Vec::new()),
    };
    let events = match result.get("events") {
        Some(e) => e,
        None => return Ok(Vec::new()),
    };

    let mut out: Vec<Action> = Vec::new();
    let mut seen: BTreeSet<(i32, &'static str)> = BTreeSet::new();

    if let Some(divs) = events.get("dividends").and_then(|d| d.as_object()) {
        for entry in divs.values() {
            let Some(ts) = entry.get("date").and_then(|d| d.as_i64()) else {
                continue;
            };
            if ts < start_unix || ts > end_unix {
                continue;
            }
            let amount = entry.get("amount").and_then(|a| a.as_f64());
            let event_date = unix_seconds_to_date32(ts);
            if !seen.insert((event_date, EVENT_CASH_DIVIDEND)) {
                continue;
            }
            out.push(Action {
                symbol: symbol.to_string(),
                event_date,
                event_type: EVENT_CASH_DIVIDEND.to_string(),
                split_ratio_num: None,
                split_ratio_den: None,
                dividend_amount: amount,
                dividend_currency: Some("USD".to_string()),
                announce_date: None,
                meta: meta.clone(),
            });
        }
    }

    if let Some(splits) = events.get("splits").and_then(|s| s.as_object()) {
        for entry in splits.values() {
            let Some(ts) = entry.get("date").and_then(|d| d.as_i64()) else {
                continue;
            };
            if ts < start_unix || ts > end_unix {
                continue;
            }
            let numerator = entry.get("numerator").and_then(|n| n.as_i64());
            let denominator = entry.get("denominator").and_then(|n| n.as_i64());
            // Some payloads only ship `splitRatio` as `"4:1"` strings;
            // parse as a fallback when the numeric fields are absent.
            let (num, den) = match (numerator, denominator) {
                (Some(n), Some(d)) => (Some(n), Some(d)),
                _ => parse_split_ratio_string(
                    entry.get("splitRatio").and_then(|s| s.as_str()),
                ),
            };
            let event_date = unix_seconds_to_date32(ts);
            if !seen.insert((event_date, EVENT_SPLIT)) {
                continue;
            }
            out.push(Action {
                symbol: symbol.to_string(),
                event_date,
                event_type: EVENT_SPLIT.to_string(),
                split_ratio_num: num,
                split_ratio_den: den,
                dividend_amount: None,
                dividend_currency: None,
                announce_date: None,
                meta: meta.clone(),
            });
        }
    }

    out.sort_by_key(|a| (a.event_date, a.event_type.clone()));
    Ok(out)
}

fn parse_split_ratio_string(s: Option<&str>) -> (Option<i64>, Option<i64>) {
    let Some(s) = s else {
        return (None, None);
    };
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return (None, None);
    }
    let n = parts[0].trim().parse::<i64>().ok();
    let d = parts[1].trim().parse::<i64>().ok();
    (n, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::yahoo_corp_actions::v1::SCHEMA_VERSION,
            1_777_300_000,
            "yahoo:chart",
        )
    }

    #[test]
    fn parses_dividend_and_split_events() {
        // Two dividends + one 4:1 split for AAPL.
        let body = r#"{
            "chart": {
                "result": [{
                    "meta": {"symbol": "AAPL"},
                    "events": {
                        "dividends": {
                            "1612224000": {"amount": 0.205, "date": 1612224000},
                            "1620086400": {"amount": 0.22, "date": 1620086400}
                        },
                        "splits": {
                            "1598832000": {
                                "date": 1598832000,
                                "numerator": 4,
                                "denominator": 1,
                                "splitRatio": "4:1"
                            }
                        }
                    }
                }],
                "error": null
            }
        }"#;
        let rows = parse_response(body, "AAPL", 0, 9_999_999_999, &meta()).expect("parse");
        assert_eq!(rows.len(), 3);
        // sorted by (event_date, event_type) — split (2020-08-31) first,
        // then two cash_dividends in 2021.
        assert_eq!(rows[0].event_type, "split");
        assert_eq!(rows[0].split_ratio_num, Some(4));
        assert_eq!(rows[0].split_ratio_den, Some(1));
        assert_eq!(rows[1].event_type, "cash_dividend");
        assert!((rows[1].dividend_amount.unwrap() - 0.205).abs() < 1e-9);
        assert_eq!(rows[1].dividend_currency.as_deref(), Some("USD"));
        assert_eq!(rows[2].event_type, "cash_dividend");
    }

    #[test]
    fn clips_events_outside_window() {
        let body = r#"{
            "chart": {
                "result": [{
                    "events": {
                        "dividends": {
                            "1000000000": {"amount": 0.1, "date": 1000000000},
                            "2000000000": {"amount": 0.2, "date": 2000000000}
                        }
                    }
                }],
                "error": null
            }
        }"#;
        let rows = parse_response(body, "AAPL", 1_500_000_000, 2_500_000_000, &meta())
            .expect("parse");
        assert_eq!(rows.len(), 1);
        assert!((rows[0].dividend_amount.unwrap() - 0.2).abs() < 1e-9);
    }

    #[test]
    fn parses_split_ratio_string_fallback() {
        let body = r#"{
            "chart": {
                "result": [{
                    "events": {
                        "splits": {
                            "1598832000": {"date": 1598832000, "splitRatio": "7:1"}
                        }
                    }
                }],
                "error": null
            }
        }"#;
        let rows = parse_response(body, "AAPL", 0, 9_999_999_999, &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].split_ratio_num, Some(7));
        assert_eq!(rows[0].split_ratio_den, Some(1));
    }

    #[test]
    fn empty_events_returns_zero_rows() {
        let body = r#"{
            "chart": {
                "result": [{
                    "meta": {"symbol": "SPY"},
                    "events": {}
                }],
                "error": null
            }
        }"#;
        let rows = parse_response(body, "SPY", 0, 9_999_999_999, &meta()).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn missing_result_returns_zero_rows() {
        // ETFs / unknown symbols may return result: null with no error.
        let body = r#"{"chart": {"result": null, "error": null}}"#;
        let rows = parse_response(body, "BOGUS", 0, 9_999_999_999, &meta()).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn surfaces_yahoo_error_envelope() {
        let body = r#"{
            "chart": {
                "result": null,
                "error": {"code": "Not Found", "description": "No data found, symbol may be delisted"}
            }
        }"#;
        let err = parse_response(body, "BOGUS", 0, 9_999_999_999, &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn malformed_json_returns_error() {
        let err = parse_response("{not json", "AAPL", 0, 9_999_999_999, &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn dedups_same_day_same_type() {
        let body = r#"{
            "chart": {
                "result": [{
                    "events": {
                        "dividends": {
                            "1612224000": {"amount": 0.20, "date": 1612224000},
                            "1612224001": {"amount": 0.20, "date": 1612224000}
                        }
                    }
                }],
                "error": null
            }
        }"#;
        let rows = parse_response(body, "AAPL", 0, 9_999_999_999, &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
    }
}
