use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate, Utc};
use clap::Parser;
use scryer_fetch_solana::pool_snapshots::{
    fetch_many, HourSignature, PoolSnapshotsFetcherConfig, PoolVaults,
};
use scryer_schema::{pool_snapshot, swap, Meta};
use scryer_store::{Dataset, UtcDay};
use serde::Deserialize;

#[derive(Parser, Debug)]
pub struct PoolSnapshotsArgs {
    /// Path to a JSON file with `pool_address`, `vault_a`, `vault_b`,
    /// optional `sol_mint` / `usdc_mint`. Same shape as
    /// `scry solana swaps`.
    #[arg(long)]
    pool_metadata: PathBuf,
    /// Window start (`YYYY-MM-DD`). Reads existing swap parquet
    /// partitions starting at this UTC day.
    #[arg(long)]
    start: String,
    /// Window end (`YYYY-MM-DD`). Inclusive.
    #[arg(long)]
    end: String,
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Venue to read existing swap parquets from. Defaults to
    /// `scryer_store::venue::SOLANA_RAYDIUM_V4`.
    #[arg(long, default_value = scryer_store::venue::SOLANA_RAYDIUM_V4)]
    source_venue: String,
    /// `_source` label stamped on every emitted row.
    #[arg(long, default_value = "rpc:getTransaction")]
    source: String,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    /// Venue under which snapshots are written. Defaults to
    /// `solana_raydium_v4` so swaps + snapshots co-locate per pool.
    #[arg(long, default_value = scryer_store::venue::SOLANA_RAYDIUM_V4)]
    venue: String,
}

#[derive(Debug, Deserialize)]
struct PoolMetadataFile {
    pool_address: String,
    vault_a: String,
    vault_b: String,
    #[serde(default)]
    sol_mint: Option<String>,
    #[serde(default)]
    usdc_mint: Option<String>,
}

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

pub async fn run_pool_snapshots(args: PoolSnapshotsArgs) -> Result<()> {
    let bytes = std::fs::read(&args.pool_metadata)
        .with_context(|| format!("reading {}", args.pool_metadata.display()))?;
    let pf: PoolMetadataFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing {}", args.pool_metadata.display()))?;
    let pool = PoolVaults {
        vault_sol: pf.vault_a,
        vault_usdc: pf.vault_b,
        sol_mint: pf.sol_mint.unwrap_or_else(|| WSOL_MINT.to_string()),
        usdc_mint: pf.usdc_mint.unwrap_or_else(|| USDC_MINT.to_string()),
    };
    let pool_address = pf.pool_address;

    let start_day = NaiveDate::parse_from_str(&args.start, "%Y-%m-%d")
        .with_context(|| format!("parsing --start {}", args.start))?;
    let end_day = NaiveDate::parse_from_str(&args.end, "%Y-%m-%d")
        .with_context(|| format!("parsing --end {}", args.end))?;
    if end_day < start_day {
        anyhow::bail!("--end ({}) must be >= --start ({})", args.end, args.start);
    }

    let ds = Dataset::new(&args.dataset);

    // Walk daily partitions, collect first-swap-per-hour into a flat
    // HashMap so duplicate hours across partition boundaries (rare,
    // but possible at UTC-midnight) collapse to a single entry by
    // earliest ts.
    let mut first_per_hour: HashMap<i64, (i64, String)> = HashMap::new();
    let mut day = start_day;
    while day <= end_day {
        let utc_day = UtcDay {
            year: day.year(),
            month: day.month(),
            day: day.day(),
        };
        match ds.read::<swap::v1::Swap>(&args.source_venue, Some(&pool_address), utc_day) {
            Ok(swaps) => {
                tracing::info!(
                    day = %day,
                    swaps = swaps.len(),
                    "loaded swap partition"
                );
                for s in &swaps {
                    let hour = (s.ts / 3600) * 3600;
                    match first_per_hour.get(&hour) {
                        Some((prev_ts, _)) if *prev_ts <= s.ts => {}
                        _ => {
                            first_per_hour.insert(hour, (s.ts, s.signature.clone()));
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(day = %day, error = %e, "swap partition not readable; skipping day");
            }
        }
        day = day
            .succ_opt()
            .ok_or_else(|| anyhow::anyhow!("date overflow at {day}"))?;
    }

    if first_per_hour.is_empty() {
        anyhow::bail!(
            "no swap rows found in {}/{}/swaps/v1 between {} and {} — \
             ensure `scry solana swaps` has populated this window",
            args.dataset.display(),
            args.source_venue,
            args.start,
            args.end
        );
    }

    let mut sigs: Vec<HourSignature> = first_per_hour
        .into_iter()
        .map(|(hour, (_ts, signature))| HourSignature { hour, signature })
        .collect();
    sigs.sort_by_key(|s| s.hour);

    tracing::info!(hours = sigs.len(), "fetching pool snapshots");

    let cfg = PoolSnapshotsFetcherConfig::new(args.proxy_url.clone());
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;
    let now = Utc::now();
    let meta = Meta::new(
        pool_snapshot::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    let snapshots = fetch_many(&client, &cfg, &pool, &sigs, &meta)
        .await
        .context("fetch_many")?;
    tracing::info!(snapshots = snapshots.len(), "fetched; writing");

    if snapshots.is_empty() {
        println!(
            "pool_snapshots fetched: 0 of {} hours produced a snapshot (no rows written)",
            sigs.len()
        );
        return Ok(());
    }
    let stats = ds
        .write::<pool_snapshot::v1::Snapshot>(&args.venue, Some(&pool_address), &snapshots)
        .context("Dataset::write")?;
    println!(
        "pool_snapshots fetched: rows_added={} rows_deduped={} partitions_written={} hours_attempted={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written, sigs.len()
    );
    Ok(())
}
