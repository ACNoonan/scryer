//! `scryer-fetch-cboe` — public-CSV historical-bar fetcher for CBOE
//! VIX-family + SKEW indices.
//!
//! Endpoints:
//! - `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`
//! - `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv`
//! - `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX1D_History.csv`
//! - `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv`
//! - `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX6M_History.csv`
//! - `https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv`
//!
//! Public, no auth. CSV column shapes:
//! - VIX-family: `DATE,OPEN,HIGH,LOW,CLOSE` with `MM/DD/YYYY` dates.
//! - SKEW: `DATE,SKEW` with `MM/DD/YYYY` dates.
//!
//! Each call returns the FULL history (since 1990 for VIX, since
//! 2008-2022 for various siblings) — this is a one-shot batch
//! fetcher, not a forward tape. Run on demand or weekly via launchd
//! to keep recent days fresh; dedup at the store layer collapses
//! re-fetches.
//!
//! P/C-ratio CSVs were probed at the same `daily_prices/` directory
//! and returned 403 — they're paywalled post-2019. Documented as
//! deferred in `wishlist.md` item 33.

use std::time::Duration;

use scryer_schema::cboe_indices::v1::{Bar, SCHEMA_VERSION};
use scryer_schema::Meta;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://cdn.cboe.com";
pub const SOURCE_LABEL: &str = "cboe:csv";

/// Indices Cboe currently publishes openly via the daily_prices CDN
/// path. Add new ones here as they appear.
pub const SUPPORTED_INDICES: &[&str] =
    &["VIX", "VIX9D", "VIX1D", "VIX3M", "VIX6M", "SKEW"];

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("malformed csv body: {0}")]
    MalformedBody(String),
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
            user_agent: concat!("scryer-fetch-cboe/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(250),
        }
    }
}

/// Fetch the FULL public history for one CBOE index.
pub async fn fetch_index_history(
    client: &reqwest::Client,
    cfg: &PollConfig,
    index: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/api/global/us_indices/daily_prices/{}_History.csv",
        cfg.base_url.trim_end_matches('/'),
        index
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client.get(&url).send().await;
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
            tracing::warn!(index, status, "cboe transient error; backing off");
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
        return parse_csv(&text, index, &cfg.source_label, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| FetchError::UpstreamStatus {
        status: 0,
        body_head: format!("retries exhausted for index={index}"),
    }))
}

/// Parse a CBOE CSV body into [`Bar`] rows. Public for unit tests.
/// Tolerates both shapes: `DATE,OPEN,HIGH,LOW,CLOSE` (VIX-family)
/// and `DATE,SKEW` (SKEW). Header row is detected by leading
/// `DATE,` and skipped.
pub fn parse_csv(
    body: &str,
    index: &str,
    source_label: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let mut out: Vec<Bar> = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("DATE,") || line.starts_with("Date,") {
            continue;
        }
        let cols: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if cols.is_empty() {
            continue;
        }
        let date32 = match parse_us_date_to_date32(cols[0]) {
            Some(d) => d,
            None => continue,
        };
        let bar = match cols.len() {
            // SKEW shape: DATE,VALUE
            2 => {
                let close: f64 = match cols[1].parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                Bar {
                    index: index.to_string(),
                    date: date32,
                    open: None,
                    high: None,
                    low: None,
                    close,
                    meta: Meta::new(SCHEMA_VERSION, fetched_at, source_label),
                }
            }
            // VIX-family shape: DATE,OPEN,HIGH,LOW,CLOSE
            5 => {
                let open: Option<f64> = cols[1].parse().ok();
                let high: Option<f64> = cols[2].parse().ok();
                let low: Option<f64> = cols[3].parse().ok();
                let close: f64 = match cols[4].parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                Bar {
                    index: index.to_string(),
                    date: date32,
                    open,
                    high,
                    low,
                    close,
                    meta: Meta::new(SCHEMA_VERSION, fetched_at, source_label),
                }
            }
            _ => continue,
        };
        out.push(bar);
    }
    if out.is_empty() && !body.is_empty() {
        return Err(FetchError::MalformedBody(format!(
            "no parseable rows; body_head={}",
            body_head(body)
        )));
    }
    Ok(out)
}

fn parse_us_date_to_date32(s: &str) -> Option<i32> {
    // CBOE CSVs use MM/DD/YYYY.
    let d = chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y").ok()?;
    Some(
        (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days() as i32,
    )
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vix_family_csv() {
        let body = "DATE,OPEN,HIGH,LOW,CLOSE\n\
                    01/02/1990,17.240000,17.240000,17.240000,17.240000\n\
                    01/03/1990,18.190000,18.190000,18.190000,18.190000\n";
        let rows = parse_csv(body, "VIX", "cboe:csv", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].index, "VIX");
        assert_eq!(rows[0].close, 17.24);
        assert_eq!(rows[0].open, Some(17.24));
        assert_eq!(rows[0].high, Some(17.24));
        assert_eq!(rows[0].low, Some(17.24));
        // 1990-01-02 → days since 1970-01-01
        let expected = (chrono::NaiveDate::from_ymd_opt(1990, 1, 2).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        assert_eq!(rows[0].date, expected);
    }

    #[test]
    fn parses_skew_csv_close_only() {
        let body = "DATE,SKEW\n\
                    01/02/1990,126.090000\n\
                    01/03/1990,123.340000\n";
        let rows = parse_csv(body, "SKEW", "cboe:csv", 1).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].close, 126.09);
        assert_eq!(rows[0].open, None);
        assert_eq!(rows[0].high, None);
        assert_eq!(rows[0].low, None);
    }

    #[test]
    fn rejects_unparseable_body() {
        let body = "this is not csv\nat all\n";
        let err = parse_csv(body, "VIX", "cboe:csv", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn empty_body_returns_zero_rows_not_error() {
        let rows = parse_csv("", "VIX", "cboe:csv", 1).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn skips_lines_with_unparseable_close() {
        let body = "DATE,OPEN,HIGH,LOW,CLOSE\n\
                    01/02/1990,17.24,17.24,17.24,oops\n\
                    01/03/1990,17.24,17.24,17.24,18.19\n";
        let rows = parse_csv(body, "VIX", "cboe:csv", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].close, 18.19);
    }

    #[test]
    fn skips_unknown_column_count() {
        let body = "DATE,OPEN,HIGH\n01/02/1990,17.24,17.24\n";
        let rows = parse_csv(body, "VIX", "cboe:csv", 1).unwrap_err();
        // No data rows of length 2 or 5 → MalformedBody (only header was 3-cols).
        assert!(matches!(rows, FetchError::MalformedBody(_)));
    }
}
