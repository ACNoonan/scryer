//! `scryer-fetch-databento` — Databento Historical API client.
//!
//! Wraps the official `databento` Rust SDK
//! (`HistoricalClient::timeseries::get_range`) for the
//! `cme_intraday_1m.v1` schema. CME futures (`GLBX.MDP3`) front-month
//! continuous contracts (`ES.c.0` / `NQ.c.0` / `GC.c.0` / `ZN.c.0`)
//! at 1-minute OHLCV resolution.
//!
//! # Pricing
//!
//! Pay-as-you-go against the operator's Databento account. The volume
//! we pull (4 tickers × 8 days × ~1440 bars/day ≈ 46k records per
//! daily run) is < $0.01 against typical OHLCV-1m pricing — orders
//! of magnitude under the $125 signup credit. Cost-aware logging in
//! the CLI surfaces records-per-call so the operator can audit
//! against the dashboard.
//!
//! # Symbol mapping
//!
//! - `ES=F` → `ES.c.0` (E-mini S&P 500 front-month continuous)
//! - `NQ=F` → `NQ.c.0` (E-mini Nasdaq 100)
//! - `GC=F` → `GC.c.0` (COMEX Gold)
//! - `ZN=F` → `ZN.c.0` (CBOT 10Y T-Note)
//!
//! Databento's `SType::Continuous` consumes the `.c.0` form directly;
//! the `.0` suffix means front-month roll. The schema retains the
//! yfinance-style `XX=F` symbol for downstream consumer parity.

use std::collections::HashMap;
use std::time::Duration;

use databento::dbn::{Dataset, OhlcvMsg, SType, Schema as DbnSchema};
use databento::historical::timeseries::GetRangeParams;
use databento::HistoricalClient;
use scryer_schema::cme_intraday_1m::v1::Bar;
use scryer_schema::Meta;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("databento error: {0}")]
    Databento(String),

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("api key is empty; pass --api-key or set DATABENTO_API_KEY env var")]
    NoApiKey,
}

/// Map a yfinance-style CME symbol (`ES=F`) to Databento's continuous-
/// contract syntax (`ES.c.0`). Returns `None` for symbols that don't
/// follow the `XX=F` convention so the caller can decide whether to
/// pass through verbatim.
pub fn symbol_to_databento_continuous(yf_symbol: &str) -> Option<String> {
    yf_symbol
        .strip_suffix("=F")
        .map(|root| format!("{}.c.0", root.to_uppercase()))
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub source_label: String,
    pub request_timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            source_label: "databento:glbx-mdp3".to_string(),
            request_timeout: Duration::from_secs(120),
        }
    }
}

/// One symbol's bars over the requested window. Multiple input
/// `(yf_symbol, databento_symbol)` pairs are issued one
/// `get_range` call per pair to keep the per-call billing line-item
/// auditable.
pub async fn fetch_ohlcv_1m(
    api_key: &str,
    cfg: &PollConfig,
    yf_symbol: &str,
    databento_symbol: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    meta: &Meta,
) -> Result<Vec<Bar>, FetchError> {
    if api_key.is_empty() {
        return Err(FetchError::NoApiKey);
    }
    let mut client = HistoricalClient::builder()
        .key(api_key)
        .map_err(|e| FetchError::Databento(format!("client key: {e}")))?
        .build()
        .map_err(|e| FetchError::Databento(format!("client build: {e}")))?;

    let params = GetRangeParams::builder()
        .dataset(Dataset::GlbxMdp3)
        .date_time_range(start..end)
        .symbols(databento_symbol)
        .stype_in(SType::Continuous)
        .schema(DbnSchema::Ohlcv1M)
        .build();

    let mut decoder = client
        .timeseries()
        .get_range(&params)
        .await
        .map_err(|e| FetchError::Databento(format!("get_range: {e}")))?;

    let mut out: Vec<Bar> = Vec::new();
    loop {
        match decoder.decode_record::<OhlcvMsg>().await {
            Ok(Some(rec)) => {
                // Prices are i64 fixed-point at 1e-9.
                let scale = 1e-9;
                let ts_ns: i64 = rec.hd.ts_event as i64;
                let ts = ts_ns / 1_000_000_000;
                out.push(Bar {
                    symbol: yf_symbol.to_string(),
                    ts,
                    open: rec.open as f64 * scale,
                    high: rec.high as f64 * scale,
                    low: rec.low as f64 * scale,
                    close: rec.close as f64 * scale,
                    volume: rec.volume,
                    meta: meta.clone(),
                });
            }
            Ok(None) => break,
            Err(e) => {
                return Err(FetchError::Databento(format!("decode: {e}")));
            }
        }
    }
    let _ = cfg.request_timeout; // configured but the SDK manages its own timeout
    let _ = HashMap::<(), ()>::new(); // silence unused-import path
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_mapping_covers_known_cme_futures() {
        assert_eq!(symbol_to_databento_continuous("ES=F").as_deref(), Some("ES.c.0"));
        assert_eq!(symbol_to_databento_continuous("NQ=F").as_deref(), Some("NQ.c.0"));
        assert_eq!(symbol_to_databento_continuous("GC=F").as_deref(), Some("GC.c.0"));
        assert_eq!(symbol_to_databento_continuous("ZN=F").as_deref(), Some("ZN.c.0"));
    }

    #[test]
    fn symbol_mapping_returns_none_for_non_futures() {
        assert!(symbol_to_databento_continuous("SPY").is_none());
        assert!(symbol_to_databento_continuous("BTC-USD").is_none());
    }
}
