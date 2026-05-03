//! `scry solana clmm-pool-state` — single-tick CLMM pool-state poll
//! for `clmm_pool_state.v1`. Wishlist 51c.
//!
//! On each invocation:
//!
//! 1. Resolve the pool set: from `--pools-file` if provided, else
//!    discover live via GeckoTerminal across the 8 xStock mints.
//! 2. `getMultipleAccounts(pools)` over the proxy. Returns one
//!    `context.slot` for the batch.
//! 3. `getBlockTime(slot)` for the block-time stamp on every emitted
//!    row.
//! 4. Decode each pool's account bytes via the dex-specific decoder
//!    (Whirlpool layout vs Raydium-CLMM layout).
//!
//! Output: one row per (pool, slot) under
//! `dataset/solana_dex/clmm_pool_state/v1/dex={...}/year=Y/month=M/day=D.parquet`.
//!
//! Schedule via the runner: `--once` semantics (each fire = one poll).
//! Re-runs within the same slot dedup naturally on `(pool, slot)`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_dexagg::jupiter::XSTOCK_MINTS;
use scryer_fetch_solana::clmm_pool_state::{poll_once, PollConfig, PoolTarget};
use scryer_fetch_solana::pool_discovery::{
    discover_pools_for_mints, GT_DEX_ORCA, GT_DEX_RAYDIUM_CLMM,
};
use scryer_schema::clmm_pool_state::v1 as schema;
use scryer_schema::clmm_pool_state::v1::{DEX_ORCA_WHIRLPOOLS, DEX_RAYDIUM_CLMM};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct ClmmPoolStateArgs {
    /// JSON-RPC endpoint for `getMultipleAccounts` + `getBlockTime`
    /// — the local proxy by default.
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Optional file with one `<pubkey> <dex_program>` per line
    /// where `dex_program` is `orca_whirlpools` or `raydium_clmm`.
    /// If omitted, the fetcher discovers pools live via
    /// GeckoTerminal across the 8 xStock mints.
    #[arg(long)]
    pools_file: Option<PathBuf>,

    /// Drop pools with reserve below this threshold (USD) when
    /// using GeckoTerminal discovery. Default 1000 USD — drops
    /// long-tail wrappers / sparsely-traded pools that bloat the
    /// poll set without adding analytical signal.
    #[arg(long, default_value_t = 1000.0)]
    min_reserve_usd: f64,

    /// Cap on number of pools polled per fire. `getMultipleAccounts`
    /// is hard-capped at 100 by Solana RPC; default to that.
    #[arg(long, default_value_t = 100)]
    max_pools: usize,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getMultipleAccounts:clmm-pool-state")]
    source: String,

    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,

    /// Inter-call delay between GeckoTerminal token-pools requests
    /// (milliseconds). Free tier rate-limits at ~30 req/min, so the
    /// default 1s spacing keeps us comfortably under the ceiling
    /// over an 8-mint discovery sweep.
    #[arg(long, default_value_t = 1000)]
    gt_inter_call_delay_ms: u64,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::SOLANA_DEX)]
    venue: String,
}

pub async fn run_clmm_pool_state(args: ClmmPoolStateArgs) -> Result<()> {
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
        println!("clmm-pool-state: rows_added=0 (no pools matched)");
        return Ok(());
    }
    tracing::info!(n_pools = pools.len(), "clmm-pool-state targets resolved");

    let rows = poll_once(&client, &cfg, &pools)
        .await
        .context("clmm-pool-state poll_once")?;
    if rows.is_empty() {
        println!("clmm-pool-state: rows_added=0 (no pool accounts decoded)");
        return Ok(());
    }

    // Bucket by dex_program so each DEX writes to its own
    // partition-key directory under `dex={...}`.
    let mut by_dex: HashMap<String, Vec<schema::PoolState>> = HashMap::new();
    for row in rows {
        by_dex.entry(row.dex_program.clone()).or_default().push(row);
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (dex, dex_rows) in &by_dex {
        let stats = ds
            .write::<schema::PoolState>(&args.venue, Some(dex), dex_rows)
            .with_context(|| format!("Dataset::write clmm_pool_state for dex={dex}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "clmm-pool-state: rows_added={} rows_deduped={} partitions={} pools_polled={}",
        total_added,
        total_deduped,
        total_partitions,
        by_dex.values().map(|v| v.len()).sum::<usize>()
    );
    Ok(())
}

fn load_pools_from_file(path: &std::path::Path) -> Result<Vec<PoolTarget>> {
    let body = std::fs::read_to_string(path)
        .with_context(|| format!("reading pools-file {}", path.display()))?;
    let mut out = Vec::new();
    for (i, line) in body.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.split_whitespace();
        let pubkey = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("pools-file line {} missing pubkey: {line}", i + 1))?;
        let dex = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("pools-file line {} missing dex: {line}", i + 1))?;
        let dex_static = match dex {
            "orca_whirlpools" => DEX_ORCA_WHIRLPOOLS,
            "raydium_clmm" => DEX_RAYDIUM_CLMM,
            other => {
                anyhow::bail!(
                    "pools-file line {}: unknown dex `{other}` (expected `orca_whirlpools` or `raydium_clmm`)",
                    i + 1
                );
            }
        };
        out.push(PoolTarget {
            pubkey: pubkey.to_string(),
            dex_program: dex_static,
        });
    }
    Ok(out)
}

/// Discover CLMM pools across the 8 xStock mints via GeckoTerminal,
/// filter to Whirlpool + Raydium-CLMM, deduplicate, and apply the
/// reserve / max-pool caps.
async fn discover_clmm_pools(
    client: &reqwest::Client,
    min_reserve_usd: f64,
    max_pools: usize,
    request_timeout: Duration,
    inter_call_delay: Duration,
) -> Result<Vec<PoolTarget>> {
    let mints: Vec<&str> = XSTOCK_MINTS.iter().map(|(_, mint)| *mint).collect();
    let discovered =
        discover_pools_for_mints(client, &mints, request_timeout, inter_call_delay).await;
    let mut out = Vec::new();
    for p in discovered {
        if let Some(reserve) = p.reserve_in_usd {
            if reserve < min_reserve_usd {
                continue;
            }
        }
        let dex_static = match p.dex_id.as_str() {
            GT_DEX_ORCA => DEX_ORCA_WHIRLPOOLS,
            GT_DEX_RAYDIUM_CLMM => DEX_RAYDIUM_CLMM,
            _ => continue, // skip non-CLMM pools (DLMM is 51d, others out of scope)
        };
        out.push(PoolTarget {
            pubkey: p.address,
            dex_program: dex_static,
        });
        if out.len() >= max_pools {
            break;
        }
    }
    Ok(out)
}
