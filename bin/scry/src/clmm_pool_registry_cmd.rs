//! `scry solana clmm-pool-registry` — pool identity for
//! `clmm_pool_registry.v1`. Wishlist item 59.
//!
//! Resolves the same pool set as `clmm-pool-state` (shared helpers, so
//! the two cannot drift apart on which pools they cover) and reads the
//! near-static fields that give the state tape units: token mints, mint
//! decimals, trade fee tier, tick spacing.
//!
//! Output: one row per (pool, UTC day) under
//! `dataset/solana_dex/clmm_pool_registry/v1/dex={...}/year=Y/month=M/day=D.parquet`.
//!
//! Cadence is monthly-ish, not per-slot — the underlying fields barely
//! move. Re-running on the same UTC day dedups to one row per pool, so
//! extra fires are free.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::clmm_pool_registry::fetch_registry;
use scryer_fetch_solana::clmm_pool_state::PollConfig;
use scryer_schema::clmm_pool_registry::v1 as schema;
use scryer_store::{venue, Dataset};

use crate::clmm_pool_state_cmd::{discover_clmm_pools, load_pools_from_file};

#[derive(Parser, Debug)]
pub struct ClmmPoolRegistryArgs {
    /// JSON-RPC endpoint for `getMultipleAccounts` — the local proxy
    /// by default.
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Optional file with one `<pubkey> <dex_program>` per line. Same
    /// format and same default source as `clmm-pool-state`; point both
    /// at `ops/sources/data/clmm-pools.txt` to keep them in lockstep.
    #[arg(long)]
    pools_file: Option<PathBuf>,

    #[arg(long, default_value_t = 1000.0)]
    min_reserve_usd: f64,

    /// Cap on pools per fire. Round 1 chunks at the RPC limit of 100
    /// internally, so this may exceed 100 safely.
    #[arg(long, default_value_t = 100)]
    max_pools: usize,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getMultipleAccounts:clmm-pool-registry")]
    source: String,

    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value_t = 1000)]
    gt_inter_call_delay_ms: u64,

    /// Print the resolved rows instead of writing parquet. Use this to
    /// eyeball a decode against a known pool before trusting the
    /// hand-coded account offsets.
    #[arg(long)]
    dry_run: bool,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::SOLANA_DEX)]
    venue: String,
}

pub async fn run_clmm_pool_registry(args: ClmmPoolRegistryArgs) -> Result<()> {
    let cfg = PollConfig {
        proxy_rpc_url: args.proxy_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let pools = if let Some(path) = &args.pools_file {
        load_pools_from_file(path)?
    } else {
        discover_clmm_pools(
            &client,
            args.min_reserve_usd,
            args.max_pools,
            Duration::from_secs(args.request_timeout_secs),
            Duration::from_millis(args.gt_inter_call_delay_ms),
        )
        .await?
    };
    if pools.is_empty() {
        println!("clmm-pool-registry: rows_added=0 (no pools matched)");
        return Ok(());
    }
    tracing::info!(n_pools = pools.len(), "clmm-pool-registry targets resolved");

    let rows = fetch_registry(&client, &cfg, &pools)
        .await
        .context("clmm-pool-registry fetch_registry")?;
    if rows.is_empty() {
        println!("clmm-pool-registry: rows_added=0 (no pool accounts decoded)");
        return Ok(());
    }

    if args.dry_run {
        println!(
            "{:<44} {:<16} {:<44} {:<44} {:>4} {:>4} {:>8} {:>6}",
            "pool", "dex", "token_mint_0", "token_mint_1", "d0", "d1", "fee_ppm", "tick"
        );
        for r in &rows {
            println!(
                "{:<44} {:<16} {:<44} {:<44} {:>4} {:>4} {:>8} {:>6}",
                r.pool_pubkey,
                r.dex_program,
                r.token_mint_0,
                r.token_mint_1,
                r.token_decimals_0,
                r.token_decimals_1,
                r.trade_fee_rate_ppm
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                r.tick_spacing,
            );
        }
        println!("clmm-pool-registry: dry-run, {} row(s), nothing written", rows.len());
        return Ok(());
    }

    let mut by_dex: HashMap<String, Vec<schema::PoolRegistry>> = HashMap::new();
    for row in rows {
        by_dex.entry(row.dex_program.clone()).or_default().push(row);
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (dex, dex_rows) in &by_dex {
        let stats = ds
            .write::<schema::PoolRegistry>(&args.venue, Some(dex), dex_rows)
            .with_context(|| format!("Dataset::write clmm_pool_registry for dex={dex}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "clmm-pool-registry: rows_added={} rows_deduped={} partitions={} pools_resolved={}",
        total_added,
        total_deduped,
        total_partitions,
        by_dex.values().map(|v| v.len()).sum::<usize>()
    );
    Ok(())
}
