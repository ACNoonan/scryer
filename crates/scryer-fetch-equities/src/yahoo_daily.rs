//! Yahoo Finance public daily-bar client — OHLCV + adjusted close for
//! the `yahoo.v1::Bar` schema.
//!
//! Endpoint: `GET https://query2.finance.yahoo.com/v8/finance/chart/{symbol}
//! ?interval=1d&period1=<unix>&period2=<unix>`. Same chart endpoint as
//! [`crate::yahoo_intraday`], but at `interval=1d` it additionally
//! returns an `indicators.adjclose[]` array (split/dividend-adjusted
//! close) which the Stooq path never had separately.
//!
//! # Why this exists (and supersedes Stooq for the forward poll)
//!
//! Stooq's free CSV endpoint began returning a JavaScript bot-challenge
//! interstitial in mid-2026 (`<noscript>This site requires JavaScript
//! to verify your browser</noscript>`), which the CSV decoder surfaces
//! as a malformed-body error — the `equities-daily` manifest silently
//! stalled. The Yahoo `/v8/finance/chart` endpoint at `interval=1d`
//! serves the same OHLCV from a first-party source with a browser-shaped
//! User-Agent and no key. The crate doc-comment's historical "daily bars
//! gate aggressively" note no longer holds for the chart endpoint as of
//! 2026-06 (verified live before this fetcher landed).
//!
//! # Symbol convention
//!
//! Yahoo takes the raw yfinance ticker directly — `SPY`, `BTC-USD`,
//! `ES=F`, `^VIX` — so (unlike Stooq) there is **no** symbol remapping.
//!
//! # Date bucketing
//!
//! Yahoo stamps each daily bar at the regular-session-open instant in
//! the exchange timezone (e.g. 13:30 UTC for US equities). The trading
//! date is recovered by shifting the unix timestamp by the response's
//! `meta.gmtoffset` before flooring to days, so a bar is never bucketed
//! into the wrong calendar day regardless of UTC vs exchange-local
//! boundary. `ts` is emitted as an arrow `Date32` (days since epoch),
//! matching [`Bar::ts`].

use chrono::NaiveDate;
use scryer_schema::yahoo::v1::Bar;
use scryer_schema::Meta;

use crate::{FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://query2.finance.yahoo.com";
pub const SOURCE_LABEL: &str = "yahoo:chart:v8";

/// Browser-shaped User-Agent. Yahoo's chart gate rejects generic
/// `reqwest/0.12`; this matches the corp-actions / earnings-backfill
/// fetcher defaults already in this crate.
pub const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// Fetch daily OHLCV bars for `symbol` over `[start_ymd, end_ymd]`
/// (UTC, inclusive). `start_ymd`/`end_ymd` are `YYYY-MM-DD`. Returns
/// `Ok(vec![])` when Yahoo has no bars in the window (unknown symbol or
/// empty range) so the caller's per-symbol loop continues, matching the
/// Stooq path's contract.
pub async fn fetch_bars(
    client: &reqwest::Client,
    cfg: &PollConfig,
    base_url: &str,
    symbol: &str,
    start_ymd: &str,
    end_ymd: &str,
    meta: &Meta,
) -> Result<Vec<Bar>, FetchError> {
    let period1 = ymd_to_unix_start(start_ymd)?;
    // End-inclusive: push the window to the end of `end_ymd`'s day so a
    // bar stamped at that day's open is captured.
    let period2 = ymd_to_unix_start(end_ymd)?
        .checked_add(86_400)
        .ok_or_else(|| FetchError::MalformedBody("end date overflow".to_string()))?;

    let url = format!(
        "{}/v8/finance/chart/{}",
        base_url.trim_end_matches('/'),
        symbol
    );
    let p1 = period1.to_string();
    let p2 = period2.to_string();

    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("interval", "1d"),
                ("period1", p1.as_str()),
                ("period2", p2.as_str()),
                ("includePrePost", "false"),
                ("events", "div,split"),
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
        // 429 / 5xx are transient (Yahoo per-IP throttle); retry. Other
        // 4xx (e.g. 404 delisted) fall through to the typed-error parse.
        if status == 429 || status >= 500 {
            last_err = Some(FetchError::UpstreamStatus { status, body: head(&text) });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            last_err = Some(FetchError::UpstreamStatus { status, body: head(&text) });
            // A hard 4xx won't fix itself on retry, but the body may
            // still carry a typed chart.error — try to parse it for a
            // cleaner message, else surface the status.
            return match parse_daily_chart_body(&text, symbol, meta) {
                Ok(rows) => Ok(rows),
                Err(_) => Err(last_err.take().unwrap()),
            };
        }
        return parse_daily_chart_body(&text, symbol, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("yahoo daily retries exhausted for {symbol}"))
    }))
}

/// Parse a Yahoo `/v8/finance/chart` daily response into `Bar` rows.
/// Public so tests can drive it without a live request. Bars with any
/// null OHLC are dropped (Yahoo emits nulls for non-trading days that
/// fall inside the requested window).
pub fn parse_daily_chart_body(body: &str, symbol: &str, meta: &Meta) -> Result<Vec<Bar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let chart = v
        .get("chart")
        .ok_or_else(|| FetchError::MalformedBody("missing chart".to_string()))?;
    if let Some(err) = chart.get("error") {
        if !err.is_null() {
            let code = err.get("code").and_then(|c| c.as_str()).unwrap_or("(no code)");
            let desc = err
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("(no description)");
            return Err(FetchError::UpstreamError(format!(
                "yahoo chart error for {symbol}: {code}: {desc}"
            )));
        }
    }
    let result = chart
        .get("result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing chart.result".to_string()))?;
    let Some(first) = result.first() else {
        return Ok(vec![]); // unknown symbol / empty window
    };
    let Some(timestamps) = first.get("timestamp").and_then(|t| t.as_array()) else {
        return Ok(vec![]); // valid response, no bars in window
    };
    // Exchange UTC offset (seconds); used to bucket the open instant
    // into the correct trading date. Absent → assume UTC.
    let gmtoffset = first
        .get("meta")
        .and_then(|m| m.get("gmtoffset"))
        .and_then(|g| g.as_i64())
        .unwrap_or(0);

    let quote = first
        .get("indicators")
        .and_then(|i| i.get("quote"))
        .and_then(|q| q.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| FetchError::MalformedBody("missing indicators.quote[0]".to_string()))?;
    let opens = arr(quote, "open")?;
    let highs = arr(quote, "high")?;
    let lows = arr(quote, "low")?;
    let closes = arr(quote, "close")?;
    let volumes = arr(quote, "volume")?;
    // adjclose is a sibling of quote; absent for some symbols (e.g.
    // futures/indices) — fall back to raw close when missing.
    let adjcloses = first
        .get("indicators")
        .and_then(|i| i.get("adjclose"))
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|o| o.get("adjclose"))
        .and_then(|x| x.as_array());

    let n = timestamps.len();
    if [opens.len(), highs.len(), lows.len(), closes.len(), volumes.len()]
        .iter()
        .any(|&l| l != n)
    {
        return Err(FetchError::MalformedBody(format!(
            "indicator arrays mismatch for {symbol}: ts={n} o={} h={} l={} c={} v={}",
            opens.len(), highs.len(), lows.len(), closes.len(), volumes.len()
        )));
    }

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let Some(ts_unix) = timestamps[i].as_i64() else { continue };
        let (Some(o), Some(h), Some(l), Some(c)) = (
            opens[i].as_f64(),
            highs[i].as_f64(),
            lows[i].as_f64(),
            closes[i].as_f64(),
        ) else {
            continue; // non-trading day inside window → null OHLC
        };
        let volume = volumes[i].as_i64().unwrap_or(0);
        let adj_close = adjcloses
            .and_then(|a| a.get(i))
            .and_then(|x| x.as_f64())
            .unwrap_or(c);
        let ts = unix_to_date32(ts_unix, gmtoffset);
        out.push(Bar {
            symbol: symbol.to_string(),
            ts,
            open: o,
            high: h,
            low: l,
            close: c,
            adj_close,
            volume,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

fn arr<'a>(quote: &'a serde_json::Value, key: &str) -> Result<&'a Vec<serde_json::Value>, FetchError> {
    quote
        .get(key)
        .and_then(|x| x.as_array())
        .ok_or_else(|| FetchError::MalformedBody(format!("missing quote.{key}")))
}

/// Shift a unix-seconds open instant by the exchange offset and floor
/// to days-since-epoch (arrow `Date32`).
fn unix_to_date32(ts_unix: i64, gmtoffset_secs: i64) -> i32 {
    ((ts_unix + gmtoffset_secs).div_euclid(86_400)) as i32
}

fn ymd_to_unix_start(ymd: &str) -> Result<i64, FetchError> {
    let d = NaiveDate::parse_from_str(ymd, "%Y-%m-%d")
        .map_err(|e| FetchError::MalformedBody(format!("expected YYYY-MM-DD, got {ymd}: {e}")))?;
    Ok(d.and_hms_opt(0, 0, 0)
        .expect("midnight is valid")
        .and_utc()
        .timestamp())
}

fn head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(scryer_schema::yahoo::v1::SCHEMA_VERSION, 1_700_000_000, SOURCE_LABEL)
    }

    #[test]
    fn parses_daily_bars_with_adjclose() {
        // Two US-equity daily bars; open instants 13:30 UTC, gmtoffset
        // -14400 (EDT). 1748266200 = 2025-05-26 13:30 UTC.
        let body = r#"{"chart":{"result":[{
          "meta":{"symbol":"SPY","gmtoffset":-14400},
          "timestamp":[1748266200, 1748352600],
          "indicators":{
            "quote":[{
              "open":[100.0, 101.0],
              "high":[101.0, 102.0],
              "low":[ 99.0, 100.5],
              "close":[100.5, 101.5],
              "volume":[1000, 2000]
            }],
            "adjclose":[{"adjclose":[100.4, 101.4]}]
          }
        }],"error":null}}"#;
        let bars = parse_daily_chart_body(body, "SPY", &meta()).expect("parse");
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].symbol, "SPY");
        // (1748266200 - 14400) / 86400 floored = 2025-05-26 = 20234 days.
        assert_eq!(bars[0].ts, 20234);
        assert!((bars[0].open - 100.0).abs() < 1e-9);
        assert!((bars[0].adj_close - 100.4).abs() < 1e-9);
        assert_eq!(bars[1].volume, 2000);
    }

    #[test]
    fn falls_back_to_close_when_adjclose_missing() {
        let body = r#"{"chart":{"result":[{
          "meta":{"symbol":"ES=F","gmtoffset":0},
          "timestamp":[1748217600],
          "indicators":{"quote":[{
            "open":[5000.0],"high":[5010.0],"low":[4990.0],
            "close":[5005.0],"volume":[123]
          }]}
        }],"error":null}}"#;
        let bars = parse_daily_chart_body(body, "ES=F", &meta()).expect("parse");
        assert_eq!(bars.len(), 1);
        assert!((bars[0].adj_close - 5005.0).abs() < 1e-9);
    }

    #[test]
    fn drops_null_ohlc_days() {
        let body = r#"{"chart":{"result":[{
          "meta":{"gmtoffset":0},
          "timestamp":[86400, 172800, 259200],
          "indicators":{"quote":[{
            "open":[10.0, null, 11.0],
            "high":[10.5, null, 11.5],
            "low":[ 9.5, null, 10.5],
            "close":[10.2, null, 11.2],
            "volume":[100, null, 200]
          }]}
        }],"error":null}}"#;
        let bars = parse_daily_chart_body(body, "X", &meta()).expect("parse");
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ts, 1);
        assert_eq!(bars[1].ts, 3);
    }

    #[test]
    fn empty_result_is_ok_empty() {
        let body = r#"{"chart":{"result":[],"error":null}}"#;
        assert!(parse_daily_chart_body(body, "X", &meta()).unwrap().is_empty());
    }

    #[test]
    fn missing_timestamp_is_ok_empty() {
        let body = r#"{"chart":{"result":[{"meta":{"gmtoffset":0}}],"error":null}}"#;
        assert!(parse_daily_chart_body(body, "X", &meta()).unwrap().is_empty());
    }

    #[test]
    fn chart_error_surfaces() {
        let body = r#"{"chart":{"result":null,"error":{
          "code":"Not Found","description":"No data found, symbol may be delisted"}}}"#;
        let err = parse_daily_chart_body(body, "BAD", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }
}
