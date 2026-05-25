//! `scryer-fetch-equities` — equity market-data fetchers.
//!
//! Two upstreams, two modules, one schema family:
//!
//! - [`stooq`] — `https://stooq.com/q/d/l/?s={symbol}&d1=YYYYMMDD&d2=YYYYMMDD&i=d`
//!   returns CSV daily OHLCV bars. No auth, no crumb, no bot detection.
//!   Symbol mapping: equities `{ticker}.us`, futures `{ticker}.f`,
//!   indices `^{ticker}`, crypto `btcusd`.
//! - [`finnhub`] — `https://finnhub.io/api/v1/calendar/earnings?from=...&to=...&symbol=...&token=KEY`
//!   returns JSON earnings calendar entries. Free tier (60 calls/min)
//!   requires an API token via env var or CLI flag.
//!
//! Output rows reuse the existing `yahoo.v1::Bar` and `earnings.v1::Event`
//! schemas (the schema names are historical — locked at lock time
//! when soothsayer's yfinance parquet was the source). The
//! `_source` column carries the actual upstream identifier
//! (`"stooq:csv"`, `"finnhub:earnings"`) so consumers can disambiguate.
//!
//! # Why not Yahoo Finance directly
//!
//! Yahoo's `/v8/finance/chart` and `/v10/finance/quoteSummary` endpoints
//! gate on a per-IP "crumb" handshake that Yahoo's bot detection
//! invalidates aggressively — a single home IP under a daily-cadence
//! refresh trips the throttle and gets `429 Too Many Requests` (and,
//! variably, `401 Invalid Cookie` from non-browser TLS clients). The
//! `yfinance` Python library handles this by silently re-trying every
//! few months when Yahoo changes the rules; we want a stable upstream
//! we don't have to babysit. Stooq + Finnhub satisfy that.

pub mod finnhub;
pub mod stooq;
pub mod yahoo_corp_actions;
pub mod yahoo_earnings;
pub mod yahoo_intraday;

use std::time::Duration;

use thiserror::Error;

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
    /// Stamped on every emitted row's `_source`.
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    /// Inter-call delay applied by the CLI between successive symbol
    /// fetches. Provider-specific defaults (e.g. Finnhub's 60/min
    /// cap → 1100ms minimum spacing) live in the CLI.
    pub rate_limit_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            source_label: "equities:rest".to_string(),
            user_agent: concat!("scryer-fetch-equities/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(500),
        }
    }
}

/// Convert a unix-second timestamp to days-since-unix-epoch (`Date32`).
pub fn unix_seconds_to_date32(unix_secs: i64) -> i32 {
    unix_secs.div_euclid(86_400) as i32
}

/// Parse a `YYYY-MM-DD` UTC date string to a Date32 (days-since-epoch).
pub fn parse_ymd_to_date32(s: &str) -> Result<i32, FetchError> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| FetchError::MalformedBody(format!("expected YYYY-MM-DD, got {s}: {e}")))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = (d - epoch).num_days();
    Ok(days as i32)
}
