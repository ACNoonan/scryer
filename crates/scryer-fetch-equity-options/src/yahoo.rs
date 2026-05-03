//! Yahoo Finance public options-chain client for the
//! `volatility.yahoo.single_stock_iv.v2` schema.
//!
//! Endpoint: `GET https://query2.finance.yahoo.com/v7/finance/options/{symbol}`
//! (no auth, no key). Without `?date=`, Yahoo returns the next chain
//! plus the array `expirationDates` of all expiry unix timestamps.
//! With `?date=<unix_secs>` it returns only that chain.
//!
//! Two-call shape per (symbol, capture):
//!
//! 1. First call with no date → pick the smallest expiry where
//!    `expiry > capture_ts + 7 days`. Yahoo's default chain is the
//!    front-week one, so on Wed/Thu/Fri it's almost always too close
//!    and we make a second call.
//! 2. Second call with `?date=<chosen>` if the first chain didn't
//!    already match.
//!
//! ATM rule (locked methodology): the strike whose absolute distance
//! from `quote.regularMarketPrice` is minimum at the chosen expiry.
//! IV is the average of the call and put `impliedVolatility` at that
//! strike when both are present, otherwise whichever side is
//! present. Yahoo returns IV as a fraction; we multiply by 100 for
//! the schema's percent-encoded `atm_iv` column.

use std::time::Duration;

use scryer_schema::single_stock_iv::v2::{SingleStockIv, SCHEMA_VERSION_YAHOO};
use scryer_schema::Meta;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://query2.finance.yahoo.com";
pub const SOURCE_LABEL: &str = "yahoo:options:v7";

/// 7 days, in seconds. The methodology lock requires
/// `expiry > capture_ts + 7d`.
pub const MIN_HORIZON_SECS: i64 = 7 * 86_400;

/// Default user-agent string. Yahoo's bot-detection wall blocks
/// generic `reqwest/0.12` on the `/v8/finance/chart` endpoint
/// (yahoo.v1::Bar was retired for this reason); the options endpoint
/// has been more permissive but a browser-like UA is the cheapest
/// hedge.
pub const DEFAULT_USER_AGENT: &str = concat!(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 ",
    "(KHTML, like Gecko) scryer-fetch-equity-options/",
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

    #[error("upstream error envelope: {0}")]
    UpstreamError(String),

    #[error("no expiry > capture_ts + {MIN_HORIZON_SECS}s available for {symbol}")]
    NoEligibleExpiry { symbol: String },

    #[error("chosen chain for {symbol} expiry {expiry_unix} has no strikes with usable IV")]
    NoUsableStrike { symbol: String, expiry_unix: i64 },

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
    pub rate_limit_delay: Duration,
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
            rate_limit_delay: Duration::from_millis(250),
        }
    }
}

/// Build a reqwest client matching the PollConfig. Centralized so
/// the CLI and tests share TLS/timeout knobs. Cookie store is
/// enabled because Yahoo's `getcrumb` flow hands back a crumb token
/// scoped to the cookies set by the prior `fc.yahoo.com` request.
pub fn build_client(cfg: &PollConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(&cfg.user_agent)
        .timeout(cfg.request_timeout)
        .cookie_store(true)
        .build()
}

/// Authenticated Yahoo session. The crumb token is bound to the
/// session cookies on `client`; both must travel together.
pub struct YahooSession {
    pub client: reqwest::Client,
    pub crumb: String,
}

/// Run the cookie+crumb dance against Yahoo. Two requests:
///
/// 1. `GET https://fc.yahoo.com/` — seeds session cookies (A1, A3,
///    GUC, B). Response body is ignored; we only need `Set-Cookie`.
/// 2. `GET {base}/v1/test/getcrumb` with cookies attached — returns
///    a plain-text crumb token.
///
/// The crumb is then appended as `?crumb=<token>` to every options
/// request. Crumbs last ~1 day; for our daily-cadence use case a
/// fresh dance per run is the simplest invariant.
///
/// This pattern is documented across yfinance (Python),
/// yahoo-finance2 (Node), and similar libraries — Yahoo started
/// requiring it on `query{1,2}.finance.yahoo.com` API endpoints in
/// 2023 and has not relaxed it.
pub async fn bootstrap_session(cfg: &PollConfig) -> Result<YahooSession, FetchError> {
    let client = build_client(cfg).map_err(FetchError::Transport)?;
    // Step 1: cookie seed. fc.yahoo.com reliably returns Set-Cookie
    // headers even when the body is a 404; we don't care about the
    // response body. We swallow non-success quietly because the
    // important state is in the cookie jar.
    let _ = client
        .get("https://fc.yahoo.com/")
        .send()
        .await
        .and_then(|r| r.error_for_status());
    // Step 2: crumb fetch. Plain-text body.
    let crumb_url = format!(
        "{}/v1/test/getcrumb",
        cfg.base_url.trim_end_matches('/')
    );
    let resp = client.get(&crumb_url).send().await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&body),
        });
    }
    let crumb = body.trim().to_string();
    if crumb.is_empty() || crumb.contains("Unauthorized") || crumb.contains('<') {
        return Err(FetchError::UpstreamError(format!(
            "yahoo getcrumb returned unusable body: {}",
            body_head(&body)
        )));
    }
    Ok(YahooSession { client, crumb })
}

/// Fetch one ATM-IV row for `symbol` at `capture_ts` (unix seconds).
/// `fetched_at` is the wall-clock the row is being recorded at — for
/// reproducibility it's typically equal to `capture_ts`.
///
/// Returns `Ok(SingleStockIv)` on success or one of the structured
/// `FetchError` variants. Retry policy is bounded by `cfg.retry_max`
/// across the full two-call sequence.
pub async fn fetch_atm_iv(
    session: &YahooSession,
    cfg: &PollConfig,
    symbol: &str,
    capture_ts: i64,
    fetched_at: i64,
) -> Result<SingleStockIv, FetchError> {
    let mut last_err: Option<String> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        match fetch_atm_iv_attempt(session, cfg, symbol, capture_ts, fetched_at).await {
            Ok(row) => return Ok(row),
            Err(e) => {
                tracing::warn!(symbol, attempt = attempt + 1, error = %e, "yahoo options poll failed");
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
        FetchError::UpstreamError(_) => true,
        FetchError::MalformedBody(_) => false,
        FetchError::NoEligibleExpiry { .. } => false,
        FetchError::NoUsableStrike { .. } => false,
        FetchError::RetriesExhausted { .. } => false,
    }
}

async fn fetch_atm_iv_attempt(
    session: &YahooSession,
    cfg: &PollConfig,
    symbol: &str,
    capture_ts: i64,
    fetched_at: i64,
) -> Result<SingleStockIv, FetchError> {
    let first = fetch_chain(session, cfg, symbol, None).await?;
    let parsed = parse_chain_envelope(&first)?;
    let expiry_unix = pick_expiry(&parsed.expiration_dates, capture_ts)
        .ok_or_else(|| FetchError::NoEligibleExpiry {
            symbol: symbol.to_string(),
        })?;
    // If the front chain happens to match, use it directly; otherwise
    // fetch the targeted chain.
    let body = if parsed.front_expiration_date == Some(expiry_unix) {
        first
    } else {
        tokio::time::sleep(cfg.rate_limit_delay).await;
        fetch_chain(session, cfg, symbol, Some(expiry_unix)).await?
    };
    build_row(
        &body,
        symbol,
        capture_ts,
        expiry_unix,
        &cfg.source_label,
        fetched_at,
    )
}

async fn fetch_chain(
    session: &YahooSession,
    cfg: &PollConfig,
    symbol: &str,
    date_unix: Option<i64>,
) -> Result<String, FetchError> {
    let url = format!(
        "{}/v7/finance/options/{}",
        cfg.base_url.trim_end_matches('/'),
        symbol
    );
    let mut req = session.client.get(&url).query(&[("crumb", session.crumb.as_str())]);
    if let Some(d) = date_unix {
        let d_str = d.to_string();
        req = req.query(&[("date", d_str.as_str())]);
    }
    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    if status == 429 || status >= 500 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    if status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    Ok(text)
}

/// Pick the smallest expiry strictly greater than `capture_ts +
/// MIN_HORIZON_SECS`. Returns `None` when no expiry satisfies the
/// threshold (very rare — implies the chain has nothing > a week
/// out).
pub fn pick_expiry(expirations: &[i64], capture_ts: i64) -> Option<i64> {
    let threshold = capture_ts.saturating_add(MIN_HORIZON_SECS);
    expirations
        .iter()
        .copied()
        .filter(|e| *e > threshold)
        .min()
}

#[derive(Clone, Debug, PartialEq)]
struct ParsedEnvelope {
    expiration_dates: Vec<i64>,
    front_expiration_date: Option<i64>,
}

/// Parse the top-level `optionChain` envelope and pull out the
/// expiration list and the front chain's expiry. Public for tests.
pub fn parse_envelope_for_test(body: &str) -> Result<(Vec<i64>, Option<i64>), FetchError> {
    let p = parse_chain_envelope(body)?;
    Ok((p.expiration_dates, p.front_expiration_date))
}

fn parse_chain_envelope(body: &str) -> Result<ParsedEnvelope, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let chain = v
        .get("optionChain")
        .ok_or_else(|| FetchError::MalformedBody("missing optionChain".to_string()))?;
    if let Some(err) = chain.get("error") {
        if !err.is_null() {
            let msg = err
                .get("description")
                .and_then(|m| m.as_str())
                .unwrap_or("(no description)");
            return Err(FetchError::UpstreamError(format!(
                "yahoo optionChain error: {msg}"
            )));
        }
    }
    let result = chain
        .get("result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing optionChain.result".to_string()))?;
    let first = result
        .first()
        .ok_or_else(|| FetchError::MalformedBody("optionChain.result is empty".to_string()))?;
    let expiration_dates = first
        .get("expirationDates")
        .and_then(|d| d.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_i64()).collect::<Vec<_>>())
        .unwrap_or_default();
    let front_expiration_date = first
        .get("options")
        .and_then(|o| o.as_array())
        .and_then(|a| a.first())
        .and_then(|c| c.get("expirationDate"))
        .and_then(|d| d.as_i64());
    Ok(ParsedEnvelope {
        expiration_dates,
        front_expiration_date,
    })
}

/// Build a single SingleStockIv row from a Yahoo response that
/// targets `expiry_unix`. Public for tests.
pub fn build_row(
    body: &str,
    symbol: &str,
    capture_ts: i64,
    expiry_unix: i64,
    source_label: &str,
    fetched_at: i64,
) -> Result<SingleStockIv, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let result = v
        .get("optionChain")
        .and_then(|c| c.get("result"))
        .and_then(|r| r.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| FetchError::MalformedBody("missing optionChain.result[0]".to_string()))?;
    let underlier_close = result
        .get("quote")
        .and_then(|q| q.get("regularMarketPrice"))
        .and_then(|p| p.as_f64());
    let chain = result
        .get("options")
        .and_then(|o| o.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| FetchError::MalformedBody("missing options[0]".to_string()))?;
    let chain_expiry = chain
        .get("expirationDate")
        .and_then(|d| d.as_i64())
        .ok_or_else(|| FetchError::MalformedBody("missing expirationDate".to_string()))?;
    if chain_expiry != expiry_unix {
        return Err(FetchError::MalformedBody(format!(
            "chain expirationDate {chain_expiry} != requested {expiry_unix}"
        )));
    }
    let calls = chain.get("calls").and_then(|c| c.as_array());
    let puts = chain.get("puts").and_then(|p| p.as_array());
    let spot = underlier_close.unwrap_or_else(|| midpoint_from_strikes(calls, puts));

    let nearest_strike = pick_nearest_strike(calls, puts, spot)
        .ok_or_else(|| FetchError::NoUsableStrike {
            symbol: symbol.to_string(),
            expiry_unix,
        })?;
    let call_iv = iv_at_strike(calls, nearest_strike);
    let put_iv = iv_at_strike(puts, nearest_strike);
    let atm_iv_fraction = match (call_iv, put_iv) {
        (Some(c), Some(p)) => Some((c + p) / 2.0),
        (Some(c), None) => Some(c),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    }
    .ok_or_else(|| FetchError::NoUsableStrike {
        symbol: symbol.to_string(),
        expiry_unix,
    })?;
    let atm_iv = atm_iv_fraction * 100.0;

    let expiry_days = (expiry_unix / 86_400) as i32;
    let days_to_expiry = (((expiry_unix - capture_ts) as f64) / 86_400.0).round() as i32;

    Ok(SingleStockIv {
        symbol: symbol.to_string(),
        ts: capture_ts,
        expiry: expiry_days,
        days_to_expiry,
        atm_iv,
        underlier_close,
        meta: Meta::new(SCHEMA_VERSION_YAHOO, fetched_at, source_label),
    })
}

/// When Yahoo returns no `quote.regularMarketPrice` (rare — halted /
/// after-hours / illiquid), fall back to the midpoint of the
/// available strike grid as a coarse spot proxy. This only steers
/// strike selection; the row's `underlier_close` stays None.
fn midpoint_from_strikes(
    calls: Option<&Vec<serde_json::Value>>,
    puts: Option<&Vec<serde_json::Value>>,
) -> f64 {
    let mut strikes: Vec<f64> = Vec::new();
    for opt in calls.iter().chain(puts.iter()).flat_map(|v| v.iter()) {
        if let Some(s) = opt.get("strike").and_then(|x| x.as_f64()) {
            strikes.push(s);
        }
    }
    if strikes.is_empty() {
        return f64::NAN;
    }
    strikes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    strikes[strikes.len() / 2]
}

fn pick_nearest_strike(
    calls: Option<&Vec<serde_json::Value>>,
    puts: Option<&Vec<serde_json::Value>>,
    spot: f64,
) -> Option<f64> {
    if !spot.is_finite() {
        return None;
    }
    let mut best: Option<(f64, f64)> = None; // (distance, strike)
    for opt in calls.iter().chain(puts.iter()).flat_map(|v| v.iter()) {
        let strike = match opt.get("strike").and_then(|x| x.as_f64()) {
            Some(s) => s,
            None => continue,
        };
        // Skip rows with no usable IV — picking a strike that has no
        // IV defeats the purpose.
        if opt.get("impliedVolatility").and_then(|x| x.as_f64()).is_none() {
            continue;
        }
        let dist = (strike - spot).abs();
        match best {
            None => best = Some((dist, strike)),
            Some((d, _)) if dist < d => best = Some((dist, strike)),
            _ => {}
        }
    }
    best.map(|(_, s)| s)
}

fn iv_at_strike(
    side: Option<&Vec<serde_json::Value>>,
    strike: f64,
) -> Option<f64> {
    let arr = side?;
    for opt in arr {
        let s = opt.get("strike").and_then(|x| x.as_f64())?;
        if (s - strike).abs() < 1e-9 {
            return opt.get("impliedVolatility").and_then(|x| x.as_f64());
        }
    }
    None
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic Yahoo response with three expirations and call+put
    /// IV at the strikes around spot.
    fn fixture_two_chain(symbol: &str, near_expiry: i64, far_expiry: i64) -> String {
        format!(
            r#"{{
              "optionChain": {{
                "result": [{{
                  "underlyingSymbol": "{symbol}",
                  "expirationDates": [{near_expiry}, {far_expiry}, {far2}],
                  "strikes": [180.0, 185.0, 190.0, 195.0, 200.0],
                  "quote": {{ "regularMarketPrice": 189.42 }},
                  "options": [{{
                    "expirationDate": {near_expiry},
                    "calls": [
                      {{"strike": 185.0, "impliedVolatility": 0.30}},
                      {{"strike": 190.0, "impliedVolatility": 0.28}},
                      {{"strike": 195.0, "impliedVolatility": 0.27}}
                    ],
                    "puts": [
                      {{"strike": 185.0, "impliedVolatility": 0.31}},
                      {{"strike": 190.0, "impliedVolatility": 0.29}},
                      {{"strike": 195.0, "impliedVolatility": 0.26}}
                    ]
                  }}]
                }}],
                "error": null
              }}
            }}"#,
            far2 = far_expiry + 7 * 86_400,
        )
    }

    #[test]
    fn pick_expiry_picks_smallest_above_threshold() {
        let capture = 1_777_500_000;
        let exps = vec![
            capture + 86_400,           // too close
            capture + 6 * 86_400,       // too close
            capture + 8 * 86_400,       // first eligible
            capture + 14 * 86_400,      // also eligible but later
        ];
        let chosen = pick_expiry(&exps, capture).unwrap();
        assert_eq!(chosen, capture + 8 * 86_400);
    }

    #[test]
    fn pick_expiry_returns_none_when_all_too_close() {
        let capture = 1_777_500_000;
        let exps = vec![capture + 1, capture + 2 * 86_400, capture + 6 * 86_400];
        assert!(pick_expiry(&exps, capture).is_none());
    }

    #[test]
    fn parse_envelope_extracts_expirations_and_front() {
        let capture = 1_777_500_000;
        let near = capture + 8 * 86_400;
        let far = capture + 30 * 86_400;
        let body = fixture_two_chain("AAPL", near, far);
        let (exps, front) = parse_envelope_for_test(&body).unwrap();
        assert_eq!(exps[0], near);
        assert_eq!(exps[1], far);
        assert_eq!(front, Some(near));
    }

    #[test]
    fn build_row_picks_nearest_strike_and_averages_call_put() {
        let capture = 1_777_500_000;
        let near = capture + 8 * 86_400;
        let far = capture + 30 * 86_400;
        let body = fixture_two_chain("AAPL", near, far);
        let row =
            build_row(&body, "AAPL", capture, near, "yahoo:test", capture).expect("row");
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.ts, capture);
        assert_eq!(row.days_to_expiry, 8);
        // expiry as days-since-epoch = near / 86400
        assert_eq!(row.expiry, (near / 86_400) as i32);
        // Spot = 189.42; nearest strike = 190.0; mean(0.28, 0.29) =
        // 0.285 → 28.5%.
        assert!((row.atm_iv - 28.5).abs() < 1e-9);
        assert_eq!(row.underlier_close, Some(189.42));
        assert_eq!(row.meta.schema_version, SCHEMA_VERSION_YAHOO);
        assert_eq!(row.meta.source, "yahoo:test");
    }

    #[test]
    fn build_row_uses_call_only_when_put_missing() {
        let capture = 1_777_500_000;
        let near = capture + 8 * 86_400;
        let body = format!(
            r#"{{
              "optionChain": {{
                "result": [{{
                  "expirationDates": [{near}],
                  "quote": {{ "regularMarketPrice": 100.0 }},
                  "options": [{{
                    "expirationDate": {near},
                    "calls": [{{"strike": 100.0, "impliedVolatility": 0.20}}],
                    "puts": []
                  }}]
                }}],
                "error": null
              }}
            }}"#
        );
        let row = build_row(&body, "X", capture, near, "yahoo:test", capture).expect("row");
        assert!((row.atm_iv - 20.0).abs() < 1e-9);
    }

    #[test]
    fn build_row_no_quote_yields_null_underlier_but_still_picks_strike() {
        let capture = 1_777_500_000;
        let near = capture + 8 * 86_400;
        let body = format!(
            r#"{{
              "optionChain": {{
                "result": [{{
                  "expirationDates": [{near}],
                  "options": [{{
                    "expirationDate": {near},
                    "calls": [
                      {{"strike": 95.0, "impliedVolatility": 0.40}},
                      {{"strike": 100.0, "impliedVolatility": 0.35}},
                      {{"strike": 105.0, "impliedVolatility": 0.38}}
                    ],
                    "puts": []
                  }}]
                }}],
                "error": null
              }}
            }}"#
        );
        // No quote → spot fallback = median strike = 100. Expect IV
        // of the 100 strike call only.
        let row = build_row(&body, "X", capture, near, "yahoo:test", capture).expect("row");
        assert_eq!(row.underlier_close, None);
        assert!((row.atm_iv - 35.0).abs() < 1e-9);
    }

    #[test]
    fn upstream_error_envelope_surfaces_as_error() {
        let body = r#"{
          "optionChain": {
            "result": [],
            "error": {"code": "Not Found", "description": "No data found, symbol may be delisted"}
          }
        }"#;
        let err = parse_envelope_for_test(body).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn build_row_rejects_chain_with_no_iv_at_any_strike() {
        let capture = 1_777_500_000;
        let near = capture + 8 * 86_400;
        let body = format!(
            r#"{{
              "optionChain": {{
                "result": [{{
                  "expirationDates": [{near}],
                  "quote": {{ "regularMarketPrice": 100.0 }},
                  "options": [{{
                    "expirationDate": {near},
                    "calls": [{{"strike": 100.0}}],
                    "puts": [{{"strike": 100.0}}]
                  }}]
                }}],
                "error": null
              }}
            }}"#
        );
        let err = build_row(&body, "X", capture, near, "yahoo:test", capture).unwrap_err();
        assert!(matches!(err, FetchError::NoUsableStrike { .. }));
    }

    #[test]
    fn build_row_rejects_chain_with_wrong_expiry() {
        let capture = 1_777_500_000;
        let near = capture + 8 * 86_400;
        let other = capture + 14 * 86_400;
        let body = format!(
            r#"{{
              "optionChain": {{
                "result": [{{
                  "expirationDates": [{other}],
                  "quote": {{ "regularMarketPrice": 100.0 }},
                  "options": [{{
                    "expirationDate": {other},
                    "calls": [{{"strike": 100.0, "impliedVolatility": 0.20}}],
                    "puts": []
                  }}]
                }}],
                "error": null
              }}
            }}"#
        );
        let err = build_row(&body, "X", capture, near, "yahoo:test", capture).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
