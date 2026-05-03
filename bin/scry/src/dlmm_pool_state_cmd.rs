//! `scry solana dlmm-pool-state` — single-tick Meteora DLMM
//! pool-state poll for `dlmm_pool_state.v1`. Wishlist 51d.
//!
//! On each invocation:
//!
//! 1. Resolve the pool set: from `--pools-file` if provided, else
//!    discover live via GeckoTerminal (`dex.id == "meteora"`)
//!    across the 8 xStock mints.
//! 2. Pass 1: `getMultipleAccounts(pools)` against the proxy →
//!    `LbPair` data. One `context.slot` for the batch.
//! 3. `getBlockTime(slot)` for the per-row timestamp.
//! 4. Derive each pool's active-bin `BinArray` PDA from
//!    `(b"bin_array", lb_pair, bin_array_index_le_bytes)`.
//! 5. Pass 2: `getMultipleAccounts(bin_arrays)` → reserve_x /
//!    reserve_y for the active bin.
//! 6. Emit one `dlmm_pool_state.v1::PoolState` row per pool whose
//!    `LbPair` AND active-bin `BinArray` both decoded successfully.
//!
//! Output: `dataset/solana_dex/dlmm_pool_state/v1/year=Y/month=M/day=D.parquet`.
//! Schedule via the runner: `--once` semantics (each fire = one
//! two-pass poll). Re-runs within the same slot dedup naturally on
//! `(pool, slot)`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_dexagg::jupiter::XSTOCK_MINTS;
use scryer_fetch_solana::dlmm_pool_state::{poll_once, PollConfig, PoolTarget};
use scryer_fetch_solana::pool_discovery::{discover_pools_for_mints, GT_DEX_METEORA};
use scryer_schema::dlmm_pool_state::v1 as schema;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct DlmmPoolStateArgs {
    /// JSON-RPC endpoint for `getMultipleAccounts` + `getBlockTime`
    /// — the local proxy by default.
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Optional file with one DLMM `<pubkey>` per line. Lines starting
    /// with `#` and blank lines are skipped. If omitted, the fetcher
    /// discovers Meteora DLMM pools live via GeckoTerminal across the
    /// 8 xStock mints (`dex.id == "meteora"`).
    #[arg(long)]
    pools_file: Option<PathBuf>,

    /// Drop pools with reserve below this threshold (USD) when using
    /// GeckoTerminal discovery. Default 1000 USD — drops long-tail
    /// wrappers / sparsely-traded pools that bloat the poll set
    /// without adding analytical signal. Mirrors the CLMM CLI.
    #[arg(long, default_value_t = 1000.0)]
    min_reserve_usd: f64,

    /// Cap on number of pools polled per fire. Each pool consumes
    /// one slot in the LbPair batch and one in the BinArray batch;
    /// `getMultipleAccounts` is hard-capped at 100 by Solana RPC, so
    /// this also caps at 100 (the runner can fire the same manifest
    /// multiple times if more pools land later).
    #[arg(long, default_value_t = 100)]
    max_pools: usize,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getMultipleAccounts:dlmm-pool-state")]
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
    /// over an 8-mint discovery sweep. Mirrors the CLMM CLI.
    #[arg(long, default_value_t = 1000)]
    gt_inter_call_delay_ms: u64,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::SOLANA_DEX)]
    venue: String,
}

pub async fn run_dlmm_pool_state(args: DlmmPoolStateArgs) -> Result<()> {
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
        discover_dlmm_pools(
            &client,
            args.min_reserve_usd,
            args.max_pools,
            Duration::from_secs(args.request_timeout_secs),
            Duration::from_millis(args.gt_inter_call_delay_ms),
        )
        .await?
    };
    if pools.is_empty() {
        println!("dlmm-pool-state: rows_added=0 (no pools matched)");
        return Ok(());
    }
    tracing::info!(n_pools = pools.len(), "dlmm-pool-state targets resolved");

    let rows = poll_once(&client, &cfg, &pools)
        .await
        .context("dlmm-pool-state poll_once")?;
    if rows.is_empty() {
        println!("dlmm-pool-state: rows_added=0 (no pool accounts decoded)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<schema::PoolState>(&args.venue, None, &rows)
        .context("Dataset::write dlmm_pool_state")?;
    println!(
        "dlmm-pool-state: rows_added={} rows_deduped={} partitions={} pools_polled={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        rows.len()
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
        // Accept `<pubkey>` or `<pubkey>  <ignored fields>` so this
        // file can carry annotations (token symbol, source, etc.)
        // without breaking the parser.
        let pubkey = line
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow::anyhow!("pools-file line {} missing pubkey: {line}", i + 1))?;
        out.push(PoolTarget {
            pubkey: pubkey.to_string(),
        });
    }
    Ok(out)
}

/// Discover DLMM pools across the 8 xStock mints via GeckoTerminal,
/// filter to Meteora-DEX pools, deduplicate, and apply the reserve /
/// max-pool caps. Mirror of `discover_clmm_pools` in
/// `clmm_pool_state_cmd.rs`.
async fn discover_dlmm_pools(
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
        if p.dex_id.as_str() != GT_DEX_METEORA {
            continue;
        }
        out.push(PoolTarget { pubkey: p.address });
        if out.len() >= max_pools {
            break;
        }
    }
    Ok(out)
}
