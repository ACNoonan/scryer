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
    fetch_ohlcv_1m, symbol_to_databento_continuous, PollConfig,
};
use scryer_schema::cme_intraday_1m::v1 as schema;
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
    /// 00:00 of `start` are emitted.
    #[arg(long)]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC. Exclusive at the day
    /// boundary (Databento convention) — bars on `end` are NOT
    /// emitted. To include `end`'s data pass the next day.
    #[arg(long)]
    end: String,
    /// Databento API key. Defaults to `DATABENTO_API_KEY` env var
    /// (loaded from `./.env` via dotenvy).
    #[arg(long, env = "DATABENTO_API_KEY")]
    api_key: String,
    #[arg(long, default_value = "databento:glbx-mdp3")]
    source: String,
    #[arg(long, default_value_t = 120)]
    request_timeout_secs: u64,
    #[arg(long, default_value = "./dataset")]
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

    let start_dt = parse_ymd_to_offset(&args.start)?;
    let end_dt = parse_ymd_to_offset(&args.end)?;

    tracing::info!(
        symbols = symbols.len(),
        start = args.start,
        end = args.end,
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

fn parse_ymd_to_offset(s: &str) -> Result<OffsetDateTime> {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {s}"))?;
    let dt = Utc
        .from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
    OffsetDateTime::from_unix_timestamp(dt.timestamp())
        .context("converting to OffsetDateTime")
}
