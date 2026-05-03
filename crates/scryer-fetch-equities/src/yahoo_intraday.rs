//! Yahoo Finance public intraday chart client — 1-minute OHLCV bars
//! for the `nasdaq_halts_intraday.v1` schema.
//!
//! Endpoint: `GET https://query2.finance.yahoo.com/v8/finance/chart/{symbol}
//! ?interval=1m&period1=<unix>&period2=<unix>`. No cookie+crumb dance
//! is needed for the chart endpoint at 1m interval as of late 2026
//! (unlike the daily-bar `yahoo.v1::Bar` path that motivated the
//! Stooq pivot — daily bars on the same endpoint started gating
//! aggressively, but the intraday chart gate has remained permissive
//! when called with a browser-shaped User-Agent).
//!
//! # Backfill horizon
//!
//! Yahoo serves 1m bars only over a 7-day rolling window. Requests
//! with `period1` older than 7 days return an empty `timestamp[]`
//! (HTTP 200, no error envelope — just empty data). The fetcher
//! treats this as `Ok(vec![])` so callers can detect "out of horizon"
//! by row count without having to special-case error variants.
//!
//! # Output shape
//!
//! [`RawBar`] carries only the per-bar fields (`ts`, OHLCV). The
//! caller attaches per-event metadata (`symbol`, `halt_event_id`,
//! [`scryer_schema::Meta`]) before writing — keeps the fetcher
//! schema-agnostic and reusable for non-halts intraday use cases.

use std::time::Duration;

use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://query2.finance.yahoo.com";
pub const SOURCE_LABEL: &str = "yahoo:chart:v8";

/// Yahoo's documented hard cap on `period1` reach-back at 1m
/// interval. Older requests come back empty.
pub const MAX_BACKFILL_SECS: i64 = 7 * 86_400;

/// Default user-agent string. Yahoo's bot detection on the chart
/// endpoint blocks generic `reqwest/0.12`; a browser-shaped UA is
/// the cheapest hedge.
pub const DEFAULT_USER_AGENT: &str = concat!(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 ",
    "(KHTML, like Gecko) scryer-fetch-equities/",
    env!("CARGO_PKG_VERSION"),
);

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("upstream error envelope: code={code}, description={description}")]
    UpstreamError { code: String, description: String },

    #[error("retries exhausted ({attempts}); last error: {last}")]
    RetriesExhausted { attempts: u32, last: String },
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub base_url: String,
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
            source_label: SOURCE_LABEL.to_string(),
            user_agent: DEFAULT_USER_AGENT.to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Build a reqwest client matching the PollConfig.
pub fn build_client(cfg: &PollConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(&cfg.user_agent)
        .timeout(cfg.request_timeout)
        .build()
}

/// One Yahoo intraday bar without scryer-side metadata. The caller
/// attaches `symbol`, `halt_event_id`, and `Meta` before writing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RawBar {
    /// Unix seconds, minute-aligned UTC.
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}

/// Fetch 1-minute bars for `symbol` in the `[period1, period2]` UTC
/// unix-seconds window. Bars outside the requested window are
/// filtered server-side; if `period1` is older than the 7-day
/// horizon the response is empty (returned as `Ok(vec![])`).
///
/// Yahoo emits null OHLC entries for minutes inside trading hours
/// where no print occurred (illiquid names, halts in progress);
/// those bars are dropped here so the writer never sees `NaN`.
pub async fn fetch_intraday_1m(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
    period1: i64,
    period2: i64,
) -> Result<Vec<RawBar>, FetchError> {
    if period2 <= period1 {
        return Err(FetchError::MalformedBody(format!(
            "period2 ({period2}) must be > period1 ({period1})"
        )));
    }
    let mut last_err: Option<String> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        match fetch_intraday_attempt(client, cfg, symbol, period1, period2).await {
            Ok(rows) => return Ok(rows),
            Err(e) => {
                tracing::warn!(symbol, attempt = attempt + 1, error = %e, "yahoo chart poll failed");
                last_err = Some(e.to_string());
                if !is_retryable(&e) {
                    return Err(e);
                }
                tokio::time::sleep(cfg.retry_delay).await;
            }
        }
    }
    Err(FetchError::RetriesExhausted {
        attempts: cfg.retry_max,
        last: last_err.unwrap_or_else(|| "unknown".to_string()),
    })
}

fn is_retryable(e: &FetchError) -> bool {
    match e {
        FetchError::Transport(_) => true,
        FetchError::UpstreamStatus { status, .. } => *status == 429 || *status >= 500,
        FetchError::UpstreamError { .. } => false,
        FetchError::MalformedBody(_) => false,
        FetchError::RetriesExhausted { .. } => false,
    }
}

async fn fetch_intraday_attempt(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
    period1: i64,
    period2: i64,
) -> Result<Vec<RawBar>, FetchError> {
    let url = format!(
        "{}/v8/finance/chart/{}",
        cfg.base_url.trim_end_matches('/'),
        symbol
    );
    let p1 = period1.to_string();
    let p2 = period2.to_string();
    let resp = client
        .get(&url)
        .query(&[
            ("interval", "1m"),
            ("period1", p1.as_str()),
            ("period2", p2.as_str()),
            // `includePrePost=false` means cash-session bars only —
            // matches the W6 analysis intent (oracle-band coverage of
            // the post-resume cash print, not after-hours quotes).
            ("includePrePost", "false"),
        ])
        .send()
        .await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    if status == 429 || status >= 500 || status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    parse_chart_body(&text)
}

/// Parse a Yahoo `/v8/finance/chart` response body. Public for tests.
pub fn parse_chart_body(body: &str) -> Result<Vec<RawBar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let chart = v
        .get("chart")
        .ok_or_else(|| FetchError::MalformedBody("missing chart".to_string()))?;
    if let Some(err) = chart.get("error") {
        if !err.is_null() {
            let code = err
                .get("code")
                .and_then(|c| c.as_str())
                .unwrap_or("(no code)")
                .to_string();
            let description = err
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("(no description)")
                .to_string();
            return Err(FetchError::UpstreamError { code, description });
        }
    }
    let result = chart
        .get("result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing chart.result".to_string()))?;
    let Some(first) = result.first() else {
        // Empty result array — Yahoo returns this when the symbol is
        // unrecognized or `period1` is outside the 7d horizon. Treat
        // as empty rather than error so backfill loops can continue.
        return Ok(vec![]);
    };
    let timestamps = match first.get("timestamp").and_then(|t| t.as_array()) {
        Some(t) => t,
        None => return Ok(vec![]), // valid response, no bars in window
    };
    let quotes = first
        .get("indicators")
        .and_then(|i| i.get("quote"))
        .and_then(|q| q.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| FetchError::MalformedBody("missing indicators.quote[0]".to_string()))?;
    let opens = quotes
        .get("open")
        .and_then(|x| x.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing quote.open".to_string()))?;
    let highs = quotes
        .get("high")
        .and_then(|x| x.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing quote.high".to_string()))?;
    let lows = quotes
        .get("low")
        .and_then(|x| x.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing quote.low".to_string()))?;
    let closes = quotes
        .get("close")
        .and_then(|x| x.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing quote.close".to_string()))?;
    let volumes = quotes
        .get("volume")
        .and_then(|x| x.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing quote.volume".to_string()))?;

    let n = timestamps.len();
    if [opens.len(), highs.len(), lows.len(), closes.len(), volumes.len()]
        .iter()
        .any(|&l| l != n)
    {
        return Err(FetchError::MalformedBody(format!(
            "indicator arrays mismatch: ts={n} open={} high={} low={} close={} volume={}",
            opens.len(),
            highs.len(),
            lows.len(),
            closes.len(),
            volumes.len(),
        )));
    }

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let Some(ts) = timestamps[i].as_i64() else {
            continue;
        };
        // Skip bars where any OHLC is null (no print this minute).
        // Volume is allowed to be 0 but must be present.
        let (Some(o), Some(h), Some(l), Some(c)) = (
            opens[i].as_f64(),
            highs[i].as_f64(),
            lows[i].as_f64(),
            closes[i].as_f64(),
        ) else {
            continue;
        };
        let v = volumes[i].as_i64().unwrap_or(0);
        out.push(RawBar {
            ts,
            open: o,
            high: h,
            low: l,
            close: c,
            volume: v,
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

    fn fixture_two_bars(t1: i64, t2: i64) -> String {
        format!(
            r#"{{"chart": {{"result": [{{
              "meta": {{"symbol": "SPY", "exchangeTimezoneName": "America/New_York"}},
              "timestamp": [{t1}, {t2}],
              "indicators": {{"quote": [{{
                "open":   [100.0, 100.5],
                "high":   [100.5, 100.7],
                "low":    [99.9,  100.4],
                "close":  [100.3, 100.6],
                "volume": [1234, 2345]
              }}]}}
            }}], "error": null}}}}"#
        )
    }

    #[test]
    fn parses_two_bars() {
        let body = fixture_two_bars(1_777_300_000, 1_777_300_060);
        let bars = parse_chart_body(&body).expect("parse");
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ts, 1_777_300_000);
        assert!((bars[0].open - 100.0).abs() < 1e-9);
        assert_eq!(bars[1].volume, 2345);
    }

    #[test]
    fn drops_null_ohlc_minutes() {
        let body = r#"{"chart":{"result":[{
          "timestamp":[1, 2, 3],
          "indicators":{"quote":[{
            "open":[10.0, null, 11.0],
            "high":[10.5, null, 11.5],
            "low": [ 9.5, null, 10.5],
            "close":[10.2, null, 11.2],
            "volume":[100, null, 200]
          }]}
        }],"error":null}}"#;
        let bars = parse_chart_body(body).expect("parse");
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ts, 1);
        assert_eq!(bars[1].ts, 3);
    }

    #[test]
    fn empty_result_means_no_bars_not_error() {
        let body = r#"{"chart":{"result":[],"error":null}}"#;
        let bars = parse_chart_body(body).expect("parse");
        assert!(bars.is_empty());
    }

    #[test]
    fn missing_timestamp_returns_empty_not_error() {
        let body = r#"{"chart":{"result":[{
          "meta":{"symbol":"SPY"}
        }],"error":null}}"#;
        let bars = parse_chart_body(body).expect("parse");
        assert!(bars.is_empty());
    }

    #[test]
    fn upstream_error_surfaces_as_typed_error() {
        let body = r#"{"chart":{"result":null,"error":{
          "code":"Not Found","description":"No data found, symbol may be delisted"
        }}}"#;
        let err = parse_chart_body(body).unwrap_err();
        match err {
            FetchError::UpstreamError { code, description } => {
                assert_eq!(code, "Not Found");
                assert!(description.contains("delisted"));
            }
            other => panic!("expected UpstreamError, got {other:?}"),
        }
    }

    #[test]
    fn indicator_array_mismatch_fails() {
        let body = r#"{"chart":{"result":[{
          "timestamp":[1, 2, 3],
          "indicators":{"quote":[{
            "open":[10.0],
            "high":[10.5],
            "low":[9.5],
            "close":[10.2],
            "volume":[100]
          }]}
        }],"error":null}}"#;
        let err = parse_chart_body(body).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
