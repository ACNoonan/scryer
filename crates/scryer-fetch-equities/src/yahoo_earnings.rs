//! Yahoo Finance earnings-history backfill fetcher.
//!
//! Endpoint: `POST https://query1.finance.yahoo.com/v1/finance/visualization?crumb=<crumb>`
//! with a JSON body that filters the `earnings` entity to one ticker.
//! This is the same upstream `yfinance`'s `Ticker.get_earnings_dates`
//! wraps, and it is the only free first-party source that returns BOTH
//! deep history AND an explicit per-event session field.
//!
//! # Why Yahoo for the backfill
//!
//! Finnhub's free `/calendar/earnings` returns no history at all
//! (verified: a purely-past window yields zero rows) — it only serves
//! the current reported quarter plus the next ~1–2 forward. So the
//! forward `session` runner (Finnhub) and the historical backfill have
//! to come from different upstreams. Yahoo's earnings visualization
//! returns `startdatetimetype` ∈ {BMO, AMC, TAS, TNS} per row going
//! back many years, which maps directly onto [`Session`].
//!
//! # The cookie + crumb gate
//!
//! Yahoo gates `query{1,2}.finance.yahoo.com` API endpoints behind a
//! cookie+crumb handshake (the same one `scryer-fetch-equity-options`
//! documents). [`bootstrap_session`] performs it once; the crumb rides
//! on every visualization POST. This is a one-shot backfill, not a
//! daily forward-tape — Yahoo's bot-detection treadmill (which drove
//! `bars`/`earnings` off Yahoo for the *recurring* path) is acceptable
//! for an occasional manual deep-history pull.
//!
//! # Session timing semantics
//!
//! `earnings_date` is the **US/Eastern** local calendar date of the
//! announcement, derived from `startdatetime` (a UTC instant) plus the
//! row's `gmtOffsetMilliSeconds`. This keeps the consumer contract
//! intact: an `amc` row dated D fires after D's close; a `bmo` row
//! dated D fires before D's open.

use std::collections::BTreeMap;

use scryer_schema::earnings::v2::{Event, Session};
use scryer_schema::Meta;

use crate::{FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://query1.finance.yahoo.com";

/// Fields requested from the visualization API. `startdatetimetype`
/// carries the session; `gmtOffsetMilliSeconds` lets us resolve the
/// US/Eastern calendar date; `epsactual` distinguishes a reported
/// quarter (confirmed timing) from a forward estimate.
const INCLUDE_FIELDS: &[&str] = &[
    "ticker",
    "startdatetime",
    "startdatetimetype",
    "epsactual",
    "epsestimate",
    "gmtOffsetMilliSeconds",
];

/// An authenticated Yahoo session: a cookie-jar client plus the crumb
/// token bound to it. Both must travel together on every request.
pub struct YahooSession {
    pub client: reqwest::Client,
    pub crumb: String,
}

/// Build a reqwest client with the cookie store enabled — required so
/// the `getcrumb` flow's session cookies persist onto the subsequent
/// visualization POSTs.
pub fn build_client(cfg: &PollConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(&cfg.user_agent)
        .timeout(cfg.request_timeout)
        .cookie_store(true)
        .build()
}

/// Run the cookie+crumb dance: seed session cookies, then fetch a
/// crumb token. `base_url` is the `query{1,2}.finance.yahoo.com` host
/// the crumb will be used against.
pub async fn bootstrap_session(
    cfg: &PollConfig,
    base_url: &str,
) -> Result<YahooSession, FetchError> {
    let client = build_client(cfg).map_err(FetchError::Transport)?;
    // Step 1: cookie seed. `fc.yahoo.com` reliably returns the A1/A3
    // session cookies via Set-Cookie even on a 404 body — this is the
    // seed the crumb is bound to (matches the proven options fetcher).
    // Only the cookie jar matters, so non-success is swallowed.
    let _ = client
        .get("https://fc.yahoo.com/")
        .send()
        .await
        .and_then(|r| r.error_for_status());
    // Step 2: crumb fetch — plain-text body bound to the cookies.
    let crumb_url = format!("{}/v1/test/getcrumb", base_url.trim_end_matches('/'));
    let resp = client.get(&crumb_url).send().await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body });
    }
    let crumb = body.trim().to_string();
    if crumb.is_empty() || crumb.contains("Unauthorized") || crumb.contains('<') {
        return Err(FetchError::UpstreamError(format!(
            "yahoo getcrumb returned unusable body: {}",
            &body.chars().take(120).collect::<String>()
        )));
    }
    Ok(YahooSession { client, crumb })
}

/// Build the visualization request body for one ticker. `size` caps the
/// number of returned events (Yahoo allows up to 250); `100` covers
/// ~25 years of quarterly reports.
fn request_body(symbol: &str, size: u32) -> serde_json::Value {
    serde_json::json!({
        "sortType": "DESC",
        "entityIdType": "earnings",
        "sortField": "startdatetime",
        "includeFields": INCLUDE_FIELDS,
        "query": {
            "operator": "and",
            "operands": [
                {"operator": "eq", "operands": ["ticker", symbol]}
            ]
        },
        "offset": 0,
        "size": size,
    })
}

/// Fetch up to `size` historical + scheduled earnings events for
/// `symbol` from Yahoo's visualization API. Empty results return
/// `Ok(Vec::new())`.
pub async fn fetch_earnings(
    session: &YahooSession,
    cfg: &PollConfig,
    base_url: &str,
    symbol: &str,
    size: u32,
    meta: &Meta,
) -> Result<Vec<Event>, FetchError> {
    let url = format!(
        "{}/v1/finance/visualization?crumb={}&lang=en-US&region=US",
        base_url.trim_end_matches('/'),
        urlencode(&session.crumb),
    );
    let body = request_body(symbol, size);
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        let resp = session
            .client
            .post(&url)
            .json(&body)
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
            // Crumb/cookie rejected — surface verbatim, don't retry.
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        if status == 429 {
            tracing::warn!(symbol, attempt = attempt + 1, "yahoo 429; backing off");
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        // Optional raw-response capture for diagnosing Yahoo schema
        // drift (the `TAS`/timestamp shape is undocumented and may
        // change). Off unless `SCRYER_YAHOO_DEBUG=<dir>` is set.
        if let Ok(path) = std::env::var("SCRYER_YAHOO_DEBUG") {
            let _ = std::fs::write(format!("{path}/yahoo_earnings_{symbol}.json"), &text);
        }
        return parse_response(&text, symbol, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("yahoo earnings retries exhausted for {symbol}"))
    }))
}

/// Parse a Yahoo earnings-visualization JSON response into [`Event`]
/// rows. Public so tests can drive it directly with canned JSON.
///
/// Rows are de-duplicated by `earnings_date` (one announcement per
/// calendar date per symbol) keeping the first occurrence; the result
/// is sorted ascending by date.
pub fn parse_response(body: &str, symbol: &str, meta: &Meta) -> Result<Vec<Event>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let finance = v
        .get("finance")
        .ok_or_else(|| FetchError::MalformedBody("missing finance key".into()))?;
    if let Some(err) = finance.get("error").filter(|e| !e.is_null()) {
        let desc = err
            .get("description")
            .and_then(|d| d.as_str())
            .or_else(|| err.as_str())
            .unwrap_or("(no description)");
        return Err(FetchError::UpstreamError(format!(
            "yahoo.visualization.error: {desc} (symbol={symbol})"
        )));
    }
    let doc = finance
        .get("result")
        .and_then(|r| r.as_array())
        .and_then(|r| r.first())
        .and_then(|r| r.get("documents"))
        .and_then(|d| d.as_array())
        .and_then(|d| d.first());
    let doc = match doc {
        Some(d) => d,
        None => return Ok(Vec::new()),
    };
    // Column id → positional index in each row array.
    let cols = doc.get("columns").and_then(|c| c.as_array());
    let Some(cols) = cols else {
        return Ok(Vec::new());
    };
    let mut idx: BTreeMap<String, usize> = BTreeMap::new();
    for (i, c) in cols.iter().enumerate() {
        if let Some(id) = c.get("id").and_then(|x| x.as_str()) {
            idx.insert(id.to_string(), i);
        }
    }
    let rows = doc.get("rows").and_then(|r| r.as_array());
    let Some(rows) = rows else {
        return Ok(Vec::new());
    };

    let get = |row: &[serde_json::Value], id: &str| -> Option<serde_json::Value> {
        idx.get(id).and_then(|&i| row.get(i)).cloned()
    };

    let mut by_date: BTreeMap<i32, Event> = BTreeMap::new();
    for row in rows {
        let Some(row) = row.as_array() else { continue };
        let Some(start) = get(row, "startdatetime") else {
            continue;
        };
        let gmt_offset_ms = get(row, "gmtOffsetMilliSeconds").and_then(|v| v.as_i64());
        let Some(local_secs) = et_local_secs(&start, gmt_offset_ms) else {
            continue;
        };
        let date32 = local_secs.div_euclid(86_400) as i32;
        let tod_minutes = (local_secs.rem_euclid(86_400) / 60) as i32;
        let row_symbol = get(row, "ticker")
            .and_then(|v| v.as_str().map(str::to_string))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| symbol.to_string());
        // Yahoo tags almost every earnings row `TAS` ("time as
        // supplied") rather than BMO/AMC, but the *timestamp* carries
        // the real session for rows from ~2015 on. So: trust an
        // explicit BMO/AMC/DMH type when present, else derive from the
        // ET time-of-day (with midnight/noon placeholders → unknown).
        let explicit = get(row, "startdatetimetype")
            .and_then(|v| v.as_str().map(str::to_string))
            .map(|s| Session::from_token(&s))
            .unwrap_or(Session::Unknown);
        let session = if explicit != Session::Unknown {
            explicit
        } else {
            derive_session(tod_minutes)
        };
        let reported = get(row, "epsactual")
            .map(|v| !v.is_null())
            .unwrap_or(false);
        by_date.entry(date32).or_insert(Event {
            symbol: row_symbol,
            earnings_date: date32,
            session,
            session_confirmed: Some(reported),
            meta: meta.clone(),
        });
    }
    Ok(by_date.into_values().collect())
}

/// Resolve the US/Eastern local seconds-since-epoch of a
/// `startdatetime` value. Handles both ISO8601 strings and integer
/// epoch-millis. Prefers the row's `gmtOffsetMilliSeconds`; falls back
/// to any offset embedded in the ISO string. The caller splits this
/// into a `Date32` (the local calendar date) and a local time-of-day.
fn et_local_secs(start: &serde_json::Value, gmt_offset_ms: Option<i64>) -> Option<i64> {
    let (utc_secs, embedded_off_secs) = if let Some(s) = start.as_str() {
        let dt = chrono::DateTime::parse_from_rfc3339(s).ok()?;
        (dt.timestamp(), dt.offset().local_minus_utc() as i64)
    } else if let Some(ms) = start.as_i64() {
        (ms / 1000, 0)
    } else {
        return None;
    };
    let off_secs = match gmt_offset_ms {
        Some(ms) if ms != 0 => ms / 1000,
        _ => embedded_off_secs,
    };
    Some(utc_secs + off_secs)
}

/// Derive a [`Session`] from the ET local time-of-day (minutes past
/// midnight) of an earnings timestamp Yahoo tagged `TAS`/`TNS`.
///
/// Midnight (`00:00`) and noon (`12:00`) on the dot are Yahoo's
/// date-only placeholders — common for pre-2015 history — and map to
/// `Unknown` rather than a bogus session. Otherwise: before 09:30 ET is
/// `bmo`, at/after 16:00 ET is `amc`, in between is `dmh`.
fn derive_session(tod_minutes: i32) -> Session {
    const MIDNIGHT: i32 = 0;
    const NOON: i32 = 12 * 60;
    const OPEN: i32 = 9 * 60 + 30;
    const CLOSE: i32 = 16 * 60;
    match tod_minutes {
        MIDNIGHT | NOON => Session::Unknown,
        t if t >= CLOSE => Session::Amc,
        t if t < OPEN => Session::Bmo,
        _ => Session::Dmh,
    }
}

/// Minimal percent-encoding for the crumb token, which can contain
/// characters (`+`, `/`, `=`) that must be escaped in a query string.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::earnings::v2::SCHEMA_VERSION,
            1_777_300_000,
            "yahoo:earnings:visualization",
        )
    }

    // A reported AMC quarter and a forward BMO quarter, in the
    // documents/columns/rows shape Yahoo's visualization API returns.
    fn body() -> String {
        r#"{
          "finance": {
            "result": [{
              "documents": [{
                "columns": [
                  {"id": "ticker"},
                  {"id": "startdatetime"},
                  {"id": "startdatetimetype"},
                  {"id": "epsactual"},
                  {"id": "epsestimate"},
                  {"id": "gmtOffsetMilliSeconds"}
                ],
                "rows": [
                  ["AAPL", "2024-02-01T21:30:00.000Z", "AMC", 2.18, 2.10, -18000000],
                  ["AAPL", "2026-05-01T11:00:00.000Z", "BMO", null, 1.50, -14400000]
                ]
              }]
            }],
            "error": null
          }
        }"#
        .to_string()
    }

    #[test]
    fn parses_session_and_confirmed() {
        let rows = parse_response(&body(), "AAPL", &meta()).expect("parse");
        assert_eq!(rows.len(), 2);
        // Sorted ascending by date. 2024-02-01 21:30Z is 16:30 ET
        // (offset -5h) → still 2024-02-01 local, AMC, reported.
        assert_eq!(rows[0].symbol, "AAPL");
        assert_eq!(rows[0].session, Session::Amc);
        assert_eq!(rows[0].session_confirmed, Some(true));
        assert_eq!(rows[0].earnings_date, days("2024-02-01"));
        // 2026-05-01 11:00Z is 07:00 ET (DST offset -4h) → 2026-05-01
        // local, BMO, forward (epsactual null).
        assert_eq!(rows[1].session, Session::Bmo);
        assert_eq!(rows[1].session_confirmed, Some(false));
        assert_eq!(rows[1].earnings_date, days("2026-05-01"));
    }

    #[test]
    fn amc_after_midnight_utc_stays_on_local_date() {
        // 2024-02-02 01:30Z = 2024-02-01 20:30 ET → local date is the
        // 1st, not the 2nd. This is the load-bearing case for the
        // consumer's single-gap mapping.
        let body = r#"{"finance":{"result":[{"documents":[{
          "columns":[{"id":"startdatetime"},{"id":"startdatetimetype"},{"id":"gmtOffsetMilliSeconds"},{"id":"epsactual"}],
          "rows":[["2024-02-02T01:30:00.000Z","AMC",-18000000,2.18]]
        }]}],"error":null}}"#;
        let rows = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].earnings_date, days("2024-02-01"));
        assert_eq!(rows[0].session, Session::Amc);
    }

    #[test]
    fn derives_amc_from_tas_real_time() {
        // Yahoo's real-world shape: type is "TAS" but the timestamp
        // carries the session. 21:31Z at -5h = 16:31 ET → after close.
        let body = r#"{"finance":{"result":[{"documents":[{
          "columns":[{"id":"startdatetime"},{"id":"startdatetimetype"},{"id":"gmtOffsetMilliSeconds"},{"id":"epsactual"}],
          "rows":[["2025-01-30T21:31:00.000Z","TAS",-18000000,2.4]]
        }]}],"error":null}}"#;
        let rows = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].session, Session::Amc);
        assert_eq!(rows[0].session_confirmed, Some(true));
    }

    #[test]
    fn derives_bmo_from_tas_premarket_time() {
        // 12:00Z at -5h = 07:00 ET → before the 09:30 open.
        let body = r#"{"finance":{"result":[{"documents":[{
          "columns":[{"id":"startdatetime"},{"id":"startdatetimetype"},{"id":"gmtOffsetMilliSeconds"}],
          "rows":[["2025-01-30T12:00:00.000Z","TAS",-18000000]]
        }]}],"error":null}}"#;
        let rows = parse_response(body, "X", &meta()).expect("parse");
        assert_eq!(rows[0].session, Session::Bmo);
    }

    #[test]
    fn midnight_and_noon_placeholders_are_unknown() {
        // Pre-2015 date-only rows: ET midnight (05:00Z at -5h) and ET
        // noon (17:00Z at -5h) are Yahoo placeholders, not real times.
        let body = r#"{"finance":{"result":[{"documents":[{
          "columns":[{"id":"startdatetime"},{"id":"startdatetimetype"},{"id":"gmtOffsetMilliSeconds"},{"id":"epsactual"}],
          "rows":[
            ["2010-01-25T05:00:00.000Z","TAS",-18000000,2.5],
            ["2010-04-25T17:00:00.000Z","TAS",-18000000,3.0]
          ]
        }]}],"error":null}}"#;
        let rows = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(rows.len(), 2);
        // Both timing-unknown, but session_confirmed=true (they did
        // report — we just don't know the session).
        assert!(rows.iter().all(|r| r.session == Session::Unknown));
        assert!(rows.iter().all(|r| r.session_confirmed == Some(true)));
    }

    #[test]
    fn explicit_type_wins_over_derivation() {
        // Explicit BMO with a 16:30 ET timestamp (which would *derive*
        // to amc) must stay bmo — the explicit tag is authoritative.
        let body = r#"{"finance":{"result":[{"documents":[{
          "columns":[{"id":"startdatetime"},{"id":"startdatetimetype"},{"id":"gmtOffsetMilliSeconds"}],
          "rows":[["2026-05-01T20:30:00.000Z","BMO",-14400000]]
        }]}],"error":null}}"#;
        let rows = parse_response(body, "X", &meta()).expect("parse");
        assert_eq!(rows[0].session, Session::Bmo);
    }

    #[test]
    fn dedups_same_local_date() {
        let body = r#"{"finance":{"result":[{"documents":[{
          "columns":[{"id":"startdatetime"},{"id":"startdatetimetype"},{"id":"gmtOffsetMilliSeconds"}],
          "rows":[["2024-02-01T21:00:00.000Z","AMC",-18000000],["2024-02-01T22:00:00.000Z","AMC",-18000000]]
        }]}],"error":null}}"#;
        let rows = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn empty_documents_returns_zero_rows() {
        let body = r#"{"finance":{"result":[],"error":null}}"#;
        let rows = parse_response(body, "AAPL", &meta()).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"finance":{"result":null,"error":{"description":"Invalid Crumb"}}}"#;
        let err = parse_response(body, "AAPL", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn malformed_json_errors() {
        let err = parse_response("{not json", "AAPL", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    fn days(ymd: &str) -> i32 {
        crate::parse_ymd_to_date32(ymd).unwrap()
    }
}
