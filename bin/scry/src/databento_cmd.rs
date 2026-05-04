//! `scry databento intraday-1m` — CME futures 1-minute OHLCV via
//! Databento Historical API.
//!
//! One Databento `get_range` call per symbol, decoded to
//! `cme_intraday_1m.v1::Bar` rows. Cost-aware: per-symbol record
//! count is logged so the operator can audit against the
//! databento.com/portal billing line items.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Utc};
use clap::Parser;
use scryer_fetch_databento::{
    fetch_equities_daily, fetch_ohlcv_1m, symbol_to_databento_continuous, PollConfig,
};
use scryer_schema::cme_intraday_1m::v1 as schema;
use scryer_schema::yahoo::v1 as yahoo_schema;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};
use time::OffsetDateTime;

/// Default symbol set Paper 1 / Paper 2 panels expect.
const DEFAULT_SYMBOLS: &[&str] = &["ES=F", "NQ=F", "GC=F", "ZN=F"];

#[derive(Parser, Debug)]
pub struct IntradayArgs {
    /// Comma-separated yfinance-style symbols. Defaults to
    /// `ES=F,NQ=F,GC=F,ZN=F`. Each must end in `=F` (mapped to
    /// Databento's `.c.0` continuous-contract syntax).
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Window start as `YYYY-MM-DD` UTC. Inclusive — bars at
    /// 00:00 of `start` are emitted. When unset, derives from
    /// `--lookback-days` before the effective end (UTC).
    #[arg(long, default_value = "")]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC. Exclusive at the day
    /// boundary (Databento convention) — bars on `end` are NOT
    /// emitted. To include `end`'s data pass the next day. When
    /// unset, defaults to `now - --end-safety-margin-secs` so a
    /// runner-driven fire captures today's bars up through the
    /// fire moment WITHOUT overshooting Databento's published-data
    /// horizon (the GLBX.MDP3 batch lags real time by a few
    /// minutes; querying past the horizon returns 422).
    #[arg(long, default_value = "")]
    end: String,
    /// Rolling-window lookback in days when `--start`/`--end` are
    /// omitted. Used by the daily forward-poll manifest so the
    /// fetch keeps pulling the most recent N days of bars without
    /// having to re-stamp dates per fire. Databento's range API
    /// returns the full window; the store dedups on `(symbol, ts)`
    /// so re-pulls are idempotent. Default 2 lets a daily fire
    /// re-cover yesterday in case the prior fire missed.
    #[arg(long, default_value_t = 2)]
    lookback_days: i64,
    /// Safety margin (seconds) subtracted from `now` when computing
    /// the default `end`. Databento's GLBX.MDP3 historical batch
    /// publishes with a small lag (~5 min observed); querying past
    /// the published horizon returns 422 Unprocessable Entity.
    /// Default 600s = 10 min keeps a comfortable margin.
    #[arg(long, default_value_t = 600)]
    end_safety_margin_secs: i64,
    /// Databento API key. Defaults to `DATABENTO_API_KEY` env var
    /// (loaded from `./.env` via dotenvy).
    #[arg(long, env = "DATABENTO_API_KEY")]
    api_key: String,
    #[arg(long, default_value = "databento:glbx-mdp3")]
    source: String,
    #[arg(long, default_value_t = 120)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CME)]
    venue: String,
}

pub async fn run_intraday(args: IntradayArgs) -> Result<()> {
    if args.api_key.is_empty() {
        anyhow::bail!(
            "Databento API key required; pass --api-key or set DATABENTO_API_KEY env var"
        );
    }
    if args.lookback_days <= 0 {
        anyhow::bail!("--lookback-days must be positive; got {}", args.lookback_days);
    }
    let cfg = PollConfig {
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let now = Utc::now();
    let meta = Meta::new(schema::SCHEMA_VERSION, now.timestamp(), &args.source);

    let symbols: Vec<String> = if args.symbols.is_empty() {
        DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect()
    } else {
        args.symbols.clone()
    };

    // End: when supplied as YYYY-MM-DD, treat as 00:00 UTC of that
    // day (exclusive day boundary, Databento convention). When
    // unset, use `now - safety_margin` directly as a sub-day
    // instant so we don't overshoot the published-data horizon.
    let (end_dt, end_label) = if args.end.is_empty() {
        let end_chrono = now - chrono::Duration::seconds(args.end_safety_margin_secs);
        let dt = OffsetDateTime::from_unix_timestamp(end_chrono.timestamp())
            .context("converting end to OffsetDateTime")?;
        (dt, end_chrono.format("%Y-%m-%dT%H:%M:%SZ").to_string())
    } else {
        (parse_ymd_to_offset(&args.end)?, args.end.clone())
    };
    // Start: when supplied as YYYY-MM-DD, treat as 00:00 UTC of
    // that day. When unset, derive `end - lookback_days`.
    let (start_dt, start_label) = if args.start.is_empty() {
        let start_chrono =
            now - chrono::Duration::seconds(args.end_safety_margin_secs)
                - chrono::Duration::days(args.lookback_days);
        let dt = OffsetDateTime::from_unix_timestamp(start_chrono.timestamp())
            .context("converting start to OffsetDateTime")?;
        (dt, start_chrono.format("%Y-%m-%dT%H:%M:%SZ").to_string())
    } else {
        (parse_ymd_to_offset(&args.start)?, args.start.clone())
    };

    tracing::info!(
        symbols = symbols.len(),
        start = start_label,
        end = end_label,
        "Databento ohlcv-1m batch"
    );

    let mut by_symbol: BTreeMap<String, Vec<schema::Bar>> = BTreeMap::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    let mut total_records = 0usize;
    for yf_sym in &symbols {
        let dbn_sym = match symbol_to_databento_continuous(yf_sym) {
            Some(s) => s,
            None => {
                errors.push((
                    yf_sym.clone(),
                    format!("symbol does not match `XX=F` futures convention; pass through unchanged not supported"),
                ));
                continue;
            }
        };
        tracing::info!(yf_sym, dbn_sym, "fetching");
        match fetch_ohlcv_1m(&args.api_key, &cfg, yf_sym, &dbn_sym, start_dt, end_dt, &meta).await {
            Ok(rows) => {
                tracing::info!(yf_sym, records = rows.len(), "decoded");
                total_records += rows.len();
                by_symbol.entry(yf_sym.clone()).or_default().extend(rows);
            }
            Err(e) => {
                tracing::warn!(yf_sym, error = %e, "fetch failed; continuing");
                errors.push((yf_sym.clone(), e.to_string()));
            }
        }
    }

    if by_symbol.values().all(|v| v.is_empty()) {
        if !errors.is_empty() {
            anyhow::bail!(
                "all {} symbol(s) failed; first error: {}",
                errors.len(),
                errors.first().map(|(_, e)| e.as_str()).unwrap_or("?")
            );
        }
        println!("databento intraday-1m: rows_added=0 (empty window)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (yf_sym, rows) in &by_symbol {
        if rows.is_empty() {
            continue;
        }
        let stats = ds
            .write::<schema::Bar>(&args.venue, Some(yf_sym), rows)
            .with_context(|| format!("Dataset::write cme_intraday_1m for {yf_sym}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "databento intraday-1m: rows_added={} rows_deduped={} partitions_written={} total_records={} symbols_failed={}",
        total_added, total_deduped, total_partitions, total_records, errors.len()
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct EquitiesDailyArgs {
    /// Comma-separated US equity tickers. Default: the 10 paper-1
    /// underliers (SPY/QQQ/AAPL/GOOGL/NVDA/TSLA/HOOD/MSTR/GLD/TLT).
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Window start as `YYYY-MM-DD` UTC.
    #[arg(long)]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC.
    #[arg(long)]
    end: String,
    /// Databento API key. Defaults to `DATABENTO_API_KEY` env var.
    #[arg(long, env = "DATABENTO_API_KEY")]
    api_key: String,
    #[arg(long, default_value = "databento:dbeq.basic")]
    source: String,
    #[arg(long, default_value_t = 120)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    /// Venue. Default `databento` so this lives separate from the
    /// Stooq-sourced `yahoo` venue (operators can cross-check the
    /// two sources without parquet collisions).
    #[arg(long, default_value = venue::DATABENTO)]
    venue: String,
}

const DEFAULT_EQUITY_SYMBOLS: &[&str] = &[
    "SPY", "QQQ", "AAPL", "GOOGL", "NVDA", "TSLA", "HOOD", "MSTR", "GLD", "TLT",
];

pub async fn run_equities_daily(args: EquitiesDailyArgs) -> Result<()> {
    if args.api_key.is_empty() {
        anyhow::bail!(
            "Databento API key required; pass --api-key or set DATABENTO_API_KEY env var"
        );
    }
    let cfg = PollConfig {
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let now = Utc::now();
    let meta = Meta::new(yahoo_schema::SCHEMA_VERSION, now.timestamp(), &args.source);

    let symbols: Vec<String> = if args.symbols.is_empty() {
        DEFAULT_EQUITY_SYMBOLS.iter().map(|s| s.to_string()).collect()
    } else {
        args.symbols.clone()
    };

    let start_dt = parse_ymd_to_offset(&args.start)?;
    let end_dt = parse_ymd_to_offset(&args.end)?;

    tracing::info!(
        symbols = symbols.len(),
        start = args.start,
        end = args.end,
        "Databento DBEQ.BASIC daily equities batch"
    );

    let mut by_symbol: BTreeMap<String, Vec<yahoo_schema::Bar>> = BTreeMap::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    let mut total_records = 0usize;
    for sym in &symbols {
        tracing::info!(sym, "fetching");
        match fetch_equities_daily(&args.api_key, &cfg, sym, start_dt, end_dt, &meta).await {
            Ok(rows) => {
                tracing::info!(sym, records = rows.len(), "decoded");
                total_records += rows.len();
                by_symbol.entry(sym.clone()).or_default().extend(rows);
            }
            Err(e) => {
                tracing::warn!(sym, error = %e, "fetch failed; continuing");
                errors.push((sym.clone(), e.to_string()));
            }
        }
    }

    if by_symbol.values().all(|v| v.is_empty()) {
        if !errors.is_empty() {
            anyhow::bail!(
                "all {} symbol(s) failed; first error: {}",
                errors.len(),
                errors.first().map(|(_, e)| e.as_str()).unwrap_or("?")
            );
        }
        println!("databento equities-daily: rows_added=0 (empty window)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (sym, rows) in &by_symbol {
        if rows.is_empty() {
            continue;
        }
        let stats = ds
            .write::<yahoo_schema::Bar>(&args.venue, Some(sym), rows)
            .with_context(|| format!("Dataset::write equities_daily for {sym}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "databento equities-daily: rows_added={} rows_deduped={} partitions_written={} total_records={} symbols_failed={}",
        total_added, total_deduped, total_partitions, total_records, errors.len()
    );
    Ok(())
}

fn parse_ymd_to_offset(s: &str) -> Result<OffsetDateTime> {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {s}"))?;
    let dt = Utc
        .from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
    OffsetDateTime::from_unix_timestamp(dt.timestamp())
        .context("converting to OffsetDateTime")
}
