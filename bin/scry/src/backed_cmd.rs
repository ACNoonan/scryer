//! `scry backed nav-strikes` — Backed Finance xStock indicative-quote
//! tape.
//!
//! Single-tick poll across the configured xStock symbols. Each tick
//! makes 3 calls per symbol (price-data + multiplier + system status)
//! and writes one `backed_nav_strikes.v1::Strike` row per symbol.
//!
//! Schedule via launchd at the desired cadence (typical: 60s during
//! US market hours; less frequently off-hours since the indicative
//! quote barely moves when the cash market is closed). Re-runs
//! within a single wall-clock minute dedup naturally on the
//! schema's minute-floored dedup_key.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_xstocks::{
    build_client, fetch_strike, PollConfig, DEFAULT_BASE_URL, DEFAULT_SYMBOLS, SOURCE_LABEL,
};
use scryer_schema::backed_nav_strikes;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct NavStrikesArgs {
    /// Comma-separated xStock symbols (e.g., `SPYx,QQQx,...`).
    /// Default: the canonical 8-symbol Backed registry.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    #[arg(long, default_value = SOURCE_LABEL)]
    source: String,
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,
    /// Network for the multiplier endpoint. Default `Solana`.
    #[arg(long, default_value = "Solana")]
    network: String,
    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-call delay (per HTTP request). Default 100ms keeps us
    /// well under the upstream's documented 1000-req/min cap.
    #[arg(long, default_value_t = 100)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::BACKED)]
    venue: String,
}

pub async fn run_nav_strikes(args: NavStrikesArgs) -> Result<()> {
    let cfg = PollConfig {
        base_url: args.base_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
        network: args.network.clone(),
        ..Default::default()
    };
    let client = build_client(&cfg).context("building reqwest client")?;
    let now = Utc::now();
    let fetched_at = now.timestamp();

    let symbols: Vec<String> = if args.symbols.is_empty() {
        DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect()
    } else {
        args.symbols.clone()
    };

    let mut all_rows: Vec<backed_nav_strikes::v1::Strike> = Vec::new();
    let mut per_symbol: BTreeMap<String, f64> = BTreeMap::new();
    let mut nones = 0usize;
    let mut errs = 0usize;
    for sym in &symbols {
        match fetch_strike(&client, &cfg, sym, fetched_at).await {
            Ok(Some(s)) => {
                per_symbol.insert(sym.clone(), s.nav_value);
                all_rows.push(s);
            }
            Ok(None) => {
                nones += 1;
                tracing::warn!(symbol = %sym, "xstocks api returned null quote; skipping");
            }
            Err(e) => {
                errs += 1;
                tracing::warn!(symbol = %sym, error = %e, "xstocks api fetch failed; continuing");
            }
        }
    }

    if all_rows.is_empty() {
        println!(
            "backed nav-strikes: rows_added=0 nones={nones} errors={errs} (no rows from any symbol)"
        );
        return Ok(());
    }

    let mut by_symbol: BTreeMap<String, Vec<backed_nav_strikes::v1::Strike>> = BTreeMap::new();
    for r in all_rows {
        by_symbol.entry(r.token_symbol.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (sym, rows) in &by_symbol {
        let stats = ds
            .write::<backed_nav_strikes::v1::Strike>(&args.venue, Some(sym), rows)
            .with_context(|| format!("Dataset::write symbol={sym}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "backed nav-strikes: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_symbol_quote={per_symbol:?}"
    );
    Ok(())
}
