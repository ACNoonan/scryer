//! `scry cex-stock-perp tape` — multi-venue stock-perp tape poll.
//!
//! Single-tick poll across the configured venues for the configured
//! xStock underlier set. Schedule cadence externally via launchd /
//! cron (typical: 60s).
//!
//! v1 ships 4 venues (Kraken Futures, Gate.io, OKX, Coinbase
//! International); the remaining 7 from `wishlist.md` item 45 are
//! follow-up enrichment modules.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_cex_perps::{
    build_client, coinbase_intl, gate, kraken_futures, okx, PollConfig,
};
use scryer_schema::cex_stock_perp_tape::v1::Tick;
use scryer_schema::{cex_stock_perp_tape, Meta};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct TapeArgs {
    /// Comma-separated canonical underlier symbols.
    #[arg(long, value_delimiter = ',', default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT")]
    underliers: Vec<String>,
    /// Disable Kraken Futures.
    #[arg(long, default_value_t = false)]
    no_kraken_futures: bool,
    /// Disable Gate.io.
    #[arg(long, default_value_t = false)]
    no_gate: bool,
    /// Disable OKX.
    #[arg(long, default_value_t = false)]
    no_okx: bool,
    /// Disable Coinbase International.
    #[arg(long, default_value_t = false)]
    no_coinbase_intl: bool,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-call delay within a venue's symbol loop.
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CEX_STOCK_PERP)]
    venue: String,
}

pub async fn run_tape(args: TapeArgs) -> Result<()> {
    if args.underliers.is_empty() {
        anyhow::bail!("--underliers cannot be empty");
    }
    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
        ..Default::default()
    };
    let client = build_client(&cfg).context("building reqwest client")?;
    let now = Utc::now();
    let fetched_at = now.timestamp();
    let underliers_upper: Vec<String> = args
        .underliers
        .iter()
        .map(|s| s.to_uppercase())
        .collect();

    let mut all_rows: Vec<Tick> = Vec::new();
    let mut per_venue: BTreeMap<&'static str, usize> = BTreeMap::new();

    if !args.no_kraken_futures {
        match kraken_futures::fetch_stock_perps(&client, &cfg, Some(&underliers_upper), fetched_at)
            .await
        {
            Ok(rows) => {
                tracing::info!(venue = "kraken_futures", rows = rows.len(), "decoded");
                *per_venue.entry("kraken_futures").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "kraken_futures", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_gate {
        match gate::fetch_stock_perps(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "gate", rows = rows.len(), "decoded");
                *per_venue.entry("gate").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "gate", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_okx {
        match okx::fetch_tape(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "okx", rows = rows.len(), "decoded");
                *per_venue.entry("okx").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "okx", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_coinbase_intl {
        match coinbase_intl::fetch_tape(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "coinbase_intl", rows = rows.len(), "decoded");
                *per_venue.entry("coinbase_intl").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "coinbase_intl", error = %e, "fetch failed; continuing"),
        }
    }

    if all_rows.is_empty() {
        println!("cex-stock-perp tape: rows_added=0 (no rows from any venue)");
        return Ok(());
    }

    // Stamp every row's meta.fetched_at to the snapshot time and
    // partition-write by underlier_symbol.
    let _ = Meta::new(cex_stock_perp_tape::v1::SCHEMA_VERSION, fetched_at, "");
    let mut by_underlier: BTreeMap<String, Vec<Tick>> = BTreeMap::new();
    for r in all_rows {
        by_underlier.entry(r.underlier_symbol.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (under, rows) in &by_underlier {
        let stats = ds
            .write::<Tick>(&args.venue, Some(under), rows)
            .with_context(|| format!("Dataset::write underlier={under}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "cex-stock-perp tape: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_venue={per_venue:?}"
    );
    Ok(())
}
