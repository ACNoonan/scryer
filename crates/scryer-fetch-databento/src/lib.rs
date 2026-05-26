//! `scryer-fetch-databento` — Databento Historical API client.
//!
//! Wraps the official `databento` Rust SDK
//! (`HistoricalClient::timeseries::get_range`) for the
//! `cme_intraday_1m.v1` schema. CME futures (`GLBX.MDP3`) front-month
//! continuous contracts (`ES.v.0` / `NQ.v.0` / `GC.v.0` / `ZN.v.0`)
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
//! - `ES=F` → `ES.v.0` (E-mini S&P 500 front-month continuous)
//! - `NQ=F` → `NQ.v.0` (E-mini Nasdaq 100)
//! - `GC=F` → `GC.v.0` (COMEX Gold)
//! - `ZN=F` → `ZN.v.0` (CBOT 10Y T-Note)
//!
//! Databento's `SType::Continuous` consumes the `.v.0` form directly;
//! the `.0` suffix means front-month roll. The schema retains the
//! yfinance-style `XX=F` symbol for downstream consumer parity.

use std::collections::HashMap;
use std::time::Duration;

use databento::dbn::{Dataset, OhlcvMsg, SType, Schema as DbnSchema};
use databento::historical::timeseries::GetRangeParams;
use databento::HistoricalClient;
use scryer_schema::bo_intraday_1m::v1::Bar as BoBar;
use scryer_schema::cme_intraday_1m::v1::Bar;
use scryer_schema::yahoo::v1::Bar as YahooBar;
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
/// contract syntax (`ES.v.0` — volume-rolled front-month). Returns
/// `None` for symbols that don't follow the `XX=F` convention so the
/// caller can decide whether to pass through verbatim.
///
/// **Volume-rolled, not calendar-rolled.** Databento's `.v.0` calendar
/// roll returns zero records for COMEX Gold (`GC.v.0` was empty in
/// our 2026-04-28 probe); `.v.0` works uniformly for ES / NQ / GC /
/// ZN. Volume rolling is also the more standard convention in
/// industry continuous-contract data.
pub fn symbol_to_databento_continuous(yf_symbol: &str) -> Option<String> {
    yf_symbol
        .strip_suffix("=F")
        .map(|root| format!("{}.v.0", root.to_uppercase()))
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

/// Fetch 1-minute OHLCV bars from Blue Ocean ATS (`OCEA.MEMOIR`).
///
/// Blue Ocean ATS operates Sun-Thu 8:00 PM – 4:00 AM ET (the canonical
/// US-equity overnight window). Databento's historical coverage starts
/// 2025-08-24. Symbols are raw NMS tickers (`SPY`, `AAPL`, etc.) — no
/// continuous-contract suffix — and are passed through `SType::RawSymbol`.
///
/// Pricing: $0.40/GB or included with subscription per Databento's blog
/// announcement; OHLCV-1m volume across the 10-symbol Soothsayer panel
/// over the ~37 weeks since 2025-08-24 is well under 1 GB.
pub async fn fetch_ocea_ohlcv_1m(
    api_key: &str,
    cfg: &PollConfig,
    symbol: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    meta: &Meta,
) -> Result<Vec<BoBar>, FetchError> {
    fetch_us_equity_ohlcv_1m(api_key, cfg, "OCEA.MEMOIR", symbol, start, end, meta).await
}

/// Generalized 1-minute OHLCV fetcher across any Databento US-equity
/// dataset that supports `Schema::Ohlcv1M` + `SType::RawSymbol`.
/// Used for venue probes (DBEQ.PLUS, EQUS.ALL, EQUS.MINI) without
/// committing to a separate per-dataset schema. Returns `bo_intraday_1m::v1::Bar`
/// rows so probe output is comparable to the OCEA backfill.
///
/// `dataset_code` is the Databento canonical string (`"OCEA.MEMOIR"`,
/// `"DBEQ.PLUS"`, `"EQUS.ALL"`, etc.); the SDK parses it via
/// `Dataset::from_str`. Returns FetchError::Databento if the code is
/// unrecognized.
pub async fn fetch_us_equity_ohlcv_1m(
    api_key: &str,
    cfg: &PollConfig,
    dataset_code: &str,
    symbol: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    meta: &Meta,
) -> Result<Vec<BoBar>, FetchError> {
    if api_key.is_empty() {
        return Err(FetchError::NoApiKey);
    }
    let dataset: Dataset = dataset_code
        .parse()
        .map_err(|e| FetchError::Databento(format!("unknown dataset {dataset_code}: {e}")))?;
    let mut client = HistoricalClient::builder()
        .key(api_key)
        .map_err(|e| FetchError::Databento(format!("client key: {e}")))?
        .build()
        .map_err(|e| FetchError::Databento(format!("client build: {e}")))?;

    let params = GetRangeParams::builder()
        .dataset(dataset)
        .date_time_range(start..end)
        .symbols(symbol)
        .stype_in(SType::RawSymbol)
        .schema(DbnSchema::Ohlcv1M)
        .build();

    let mut decoder = client
        .timeseries()
        .get_range(&params)
        .await
        .map_err(|e| FetchError::Databento(format!("get_range: {e}")))?;

    let mut out: Vec<BoBar> = Vec::new();
    loop {
        match decoder.decode_record::<OhlcvMsg>().await {
            Ok(Some(rec)) => {
                let scale = 1e-9;
                let ts_ns: i64 = rec.hd.ts_event as i64;
                let ts = ts_ns / 1_000_000_000;
                out.push(BoBar {
                    symbol: symbol.to_string(),
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
    let _ = cfg.request_timeout;
    Ok(out)
}

/// Fetch daily equity OHLCV bars via Databento's `DBEQ.BASIC` dataset.
/// Reuses the existing `yahoo.v1::Bar` schema (the schema name is
/// historical from soothsayer's yfinance era; the row shape is
/// "OHLCV daily bars from somewhere", upstream-agnostic).
///
/// `DBEQ.BASIC` consolidates multiple US-equity venues; the same
/// trading day for a symbol may appear as 2-4 records (one per
/// consolidated venue / SIP listing). The store's
/// `(symbol, ts)` dedup_key collapses these to one row per
/// (symbol, day) — first observation wins.
///
/// `adj_close` is set equal to `close`: Databento doesn't pre-apply
/// split/dividend adjustments. For paper-1 Stooq cross-check, both
/// sources end up with adjusted prices in `close`/`adj_close`,
/// directly comparable.
pub async fn fetch_equities_daily(
    api_key: &str,
    cfg: &PollConfig,
    symbol: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    meta: &Meta,
) -> Result<Vec<YahooBar>, FetchError> {
    if api_key.is_empty() {
        return Err(FetchError::NoApiKey);
    }
    let mut client = HistoricalClient::builder()
        .key(api_key)
        .map_err(|e| FetchError::Databento(format!("client key: {e}")))?
        .build()
        .map_err(|e| FetchError::Databento(format!("client build: {e}")))?;

    let params = GetRangeParams::builder()
        .dataset("DBEQ.BASIC")
        .date_time_range(start..end)
        .symbols(symbol)
        .stype_in(SType::RawSymbol)
        .schema(DbnSchema::Ohlcv1D)
        .build();

    let mut decoder = client
        .timeseries()
        .get_range(&params)
        .await
        .map_err(|e| FetchError::Databento(format!("get_range: {e}")))?;

    let mut out: Vec<YahooBar> = Vec::new();
    loop {
        match decoder.decode_record::<OhlcvMsg>().await {
            Ok(Some(rec)) => {
                let scale = 1e-9;
                let ts_ns: i64 = rec.hd.ts_event as i64;
                // Convert ns-since-epoch → Date32 (days-since-epoch).
                // `yahoo.v1::Bar.ts` is i32 days.
                let ts_days_i64 = ts_ns / 86_400_000_000_000;
                let ts: i32 = ts_days_i64 as i32;
                let close = rec.close as f64 * scale;
                out.push(YahooBar {
                    symbol: symbol.to_string(),
                    ts,
                    open: rec.open as f64 * scale,
                    high: rec.high as f64 * scale,
                    low: rec.low as f64 * scale,
                    close,
                    adj_close: close,
                    volume: rec.volume as i64,
                    meta: meta.clone(),
                });
            }
            Ok(None) => break,
            Err(e) => {
                return Err(FetchError::Databento(format!("decode: {e}")));
            }
        }
    }
    let _ = cfg.request_timeout;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_mapping_covers_known_cme_futures() {
        assert_eq!(symbol_to_databento_continuous("ES=F").as_deref(), Some("ES.v.0"));
        assert_eq!(symbol_to_databento_continuous("NQ=F").as_deref(), Some("NQ.v.0"));
        assert_eq!(symbol_to_databento_continuous("GC=F").as_deref(), Some("GC.v.0"));
        assert_eq!(symbol_to_databento_continuous("ZN=F").as_deref(), Some("ZN.v.0"));
        assert_eq!(symbol_to_databento_continuous("CL=F").as_deref(), Some("CL.v.0"));
        assert_eq!(symbol_to_databento_continuous("6E=F").as_deref(), Some("6E.v.0"));
    }

    #[test]
    fn symbol_mapping_returns_none_for_non_futures() {
        assert!(symbol_to_databento_continuous("SPY").is_none());
        assert!(symbol_to_databento_continuous("BTC-USD").is_none());
    }
}
