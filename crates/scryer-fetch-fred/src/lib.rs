//! `scryer-fetch-fred` — FRED macro release-calendar fetcher.
//!
//! Endpoint: `https://api.stlouisfed.org/fred/release/dates?release_id={ID}&realtime_start=YYYY-MM-DD&realtime_end=YYYY-MM-DD&file_type=json&api_key=KEY`
//!
//! REST-only, no proxy. Free API key required (register at
//! `https://fredaccount.stlouisfed.org/apikey`). Free tier rate limit
//! is 120 calls/min — well within reach for the default 6-release
//! poll.
//!
//! # Default release registry
//!
//! The canonical regime-regressor releases for Paper 1's calibration
//! pipeline. Each entry maps `(release_id, canonical_event_name)`:
//!
//! - `10` — `"CPI"` (Consumer Price Index)
//! - `50` — `"NFP"` (Employment Situation, a.k.a. Non-Farm Payrolls)
//! - `53` — `"GDP"` (Gross Domestic Product)
//! - `21` — `"PCE"` (Personal Income and Outlays)
//! - `84` — `"PPI"` (Producer Price Index)
//! - `32` — `"RetailSales"` (Retail Trade)
//!
//! FOMC meeting dates are NOT in this set — FRED's `Release` concept
//! covers data publications, not Fed monetary-policy meetings. Users
//! who need FOMC dates should add them via a separate manual-source
//! upstream (deferred to a future phase).

use std::time::Duration;

use scryer_schema::fred_macro::v1::Event;
use scryer_schema::Meta;
use thiserror::Error;

use crate::release::{ReleaseEntry, DEFAULT_RELEASES};

pub mod release;
pub mod series;

pub const DEFAULT_BASE_URL: &str = "https://api.stlouisfed.org";

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

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
    pub rate_limit_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            source_label: "fred:release_dates".to_string(),
            user_agent: concat!("scryer-fetch-fred/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            // 120 calls/min → ~500ms minimum spacing keeps us safe.
            rate_limit_delay: Duration::from_millis(500),
        }
    }
}

/// Fetch release dates for a single FRED release in `[realtime_start,
/// realtime_end]`. Returns 0..n events.
pub async fn fetch_release_dates(
    client: &reqwest::Client,
    cfg: &PollConfig,
    api_key: &str,
    entry: &ReleaseEntry,
    realtime_start: &str,
    realtime_end: &str,
    meta: &Meta,
) -> Result<Vec<Event>, FetchError> {
    if api_key.is_empty() {
        return Err(FetchError::UpstreamError(
            "fred api key is empty; pass --api-key or set FRED_API_KEY env var".to_string(),
        ));
    }
    let url = format!("{}/fred/release/dates", cfg.base_url.trim_end_matches('/'));
    let release_id_str = entry.release_id.to_string();
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("release_id", release_id_str.as_str()),
                ("realtime_start", realtime_start),
                ("realtime_end", realtime_end),
                ("include_release_dates_with_no_data", "true"),
                ("file_type", "json"),
                ("api_key", api_key),
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
        if status == 429 || status >= 500 {
            // Transient — retry. FRED occasionally emits 500
            // Internal Server Error under load.
            tracing::warn!(
                release_id = entry.release_id,
                status,
                "fred transient error; backing off"
            );
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            // Don't retry 4xx other than 429 — likely bad api key
            // or bad release_id.
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        return parse_release_dates_response(&text, entry, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!(
            "fred retries exhausted for release_id={}",
            entry.release_id
        ))
    }))
}

/// Parse the FRED `/release/dates` JSON body into [`Event`] rows.
/// Public so tests can drive it directly with hardcoded JSON strings.
pub fn parse_release_dates_response(
    body: &str,
    entry: &ReleaseEntry,
    meta: &Meta,
) -> Result<Vec<Event>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if let Some(err) = v.get("error_message").and_then(|m| m.as_str()) {
        return Err(FetchError::UpstreamError(format!(
            "fred error_message: {err} (release_id={})",
            entry.release_id
        )));
    }
    let dates = v
        .get("release_dates")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Event> = Vec::new();
    let mut seen: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    for entry_value in dates {
        let date_str = match entry_value.get("date").and_then(|d| d.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let date32 = match parse_iso_date_to_date32(date_str) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if !seen.insert(date32) {
            continue;
        }
        // Honor upstream `release_name` when present; fall back to
        // the static registry entry otherwise.
        let release_name = entry_value
            .get("release_name")
            .and_then(|n| n.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(entry.upstream_name)
            .to_string();
        out.push(Event {
            event_date: date32,
            event_name: entry.event_name.to_string(),
            release_id: Some(entry.release_id),
            release_name,
            release_source: "fred".to_string(),
            meta: meta.clone(),
        });
    }
    Ok(out)
}

/// Convenience: fetch the default 6-release set across `[start, end]`.
pub async fn fetch_default_calendar(
    client: &reqwest::Client,
    cfg: &PollConfig,
    api_key: &str,
    realtime_start: &str,
    realtime_end: &str,
    meta: &Meta,
) -> Result<Vec<Event>, FetchError> {
    let mut out = Vec::new();
    for entry in DEFAULT_RELEASES {
        let rows = fetch_release_dates(
            client,
            cfg,
            api_key,
            entry,
            realtime_start,
            realtime_end,
            meta,
        )
        .await?;
        out.extend(rows);
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }
    Ok(out)
}

fn parse_iso_date_to_date32(s: &str) -> Result<i32, FetchError> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| FetchError::MalformedBody(format!("bad ISO date {s:?}: {e}")))?;
    Ok((d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::fred_macro::v1::SCHEMA_VERSION,
            1_777_300_000,
            "fred:release_dates",
        )
    }

    fn cpi_entry() -> ReleaseEntry {
        ReleaseEntry {
            release_id: 10,
            event_name: "CPI",
            upstream_name: "Consumer Price Index",
        }
    }

    #[test]
    fn parses_typical_response() {
        let body = r#"{
            "realtime_start": "2026-01-01",
            "realtime_end": "2026-12-31",
            "release_dates": [
                {"release_id": 10, "release_name": "Consumer Price Index", "date": "2026-01-15"},
                {"release_id": 10, "release_name": "Consumer Price Index", "date": "2026-02-12"},
                {"release_id": 10, "release_name": "Consumer Price Index", "date": "2026-03-12"}
            ]
        }"#;
        let events = parse_release_dates_response(body, &cpi_entry(), &meta()).expect("parse");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event_name, "CPI");
        assert_eq!(events[0].release_id, Some(10));
        assert_eq!(events[0].release_name, "Consumer Price Index");
        assert_eq!(events[0].release_source, "fred");
        // 2026-01-15 → 20468 days
        assert_eq!(events[0].event_date, 20_468);
    }

    #[test]
    fn dedups_duplicate_dates() {
        let body = r#"{
            "release_dates": [
                {"release_id": 10, "date": "2026-01-15"},
                {"release_id": 10, "date": "2026-01-15"}
            ]
        }"#;
        let events = parse_release_dates_response(body, &cpi_entry(), &meta()).expect("parse");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn falls_back_to_registry_release_name_when_upstream_missing() {
        let body = r#"{
            "release_dates": [
                {"release_id": 10, "date": "2026-01-15"}
            ]
        }"#;
        let events = parse_release_dates_response(body, &cpi_entry(), &meta()).expect("parse");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].release_name, "Consumer Price Index");
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"error_message": "Bad API key", "error_code": 400}"#;
        let err = parse_release_dates_response(body, &cpi_entry(), &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn missing_release_dates_field_returns_zero_rows() {
        let body = r#"{"realtime_start": "2026-01-01", "realtime_end": "2026-12-31"}"#;
        let events = parse_release_dates_response(body, &cpi_entry(), &meta()).expect("parse");
        assert!(events.is_empty());
    }

    #[test]
    fn malformed_json_returns_error() {
        let err = parse_release_dates_response("{not json", &cpi_entry(), &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
