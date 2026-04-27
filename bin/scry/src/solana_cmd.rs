use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::{mints, PoolMetadata, SwapsFetcher, SwapsFetcherConfig};
use scryer_schema::swap;
use scryer_store::Dataset;
use serde::Deserialize;

#[derive(Parser, Debug)]
pub struct SwapsArgs {
    /// Path to a JSON file with `pool_address`, `vault_a` (WSOL),
    /// `vault_b` (USDC), optional `sol_mint` / `usdc_mint`. Matches
    /// `quant-work/data/pool_metadata.json` shape.
    #[arg(long)]
    pool_metadata: PathBuf,
    /// Window start. `YYYY-MM-DD`, RFC 3339, or unix seconds.
    #[arg(long)]
    start: String,
    /// Window end. Same formats as `--start`.
    #[arg(long)]
    end: String,
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Helius API key for the parseTransactions call. Overrides
    /// `HELIUS_API_KEY` env var.
    #[arg(long, env = "HELIUS_API_KEY")]
    helius_api_key: String,
    /// Output dataset root.
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    /// Venue string under `dataset/`. Defaults to
    /// `scryer_store::venue::SOLANA_RAYDIUM_V4`.
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

pub async fn run_swaps(args: SwapsArgs) -> Result<()> {
    let bytes = std::fs::read(&args.pool_metadata)
        .with_context(|| format!("reading {}", args.pool_metadata.display()))?;
    let meta: PoolMetadataFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing {}", args.pool_metadata.display()))?;

    let pool = PoolMetadata {
        pool_address: meta.pool_address,
        vault_sol: meta.vault_a,
        vault_usdc: meta.vault_b,
        sol_mint: meta.sol_mint.unwrap_or_else(|| mints::WSOL.to_string()),
        usdc_mint: meta.usdc_mint.unwrap_or_else(|| mints::USDC.to_string()),
    };

    let start_ts = crate::parse_unix_seconds(&args.start).context("parsing --start")?;
    let end_ts = crate::parse_unix_seconds(&args.end).context("parsing --end")?;
    if end_ts <= start_ts {
        anyhow::bail!("--end ({end_ts}) must be > --start ({start_ts})");
    }

    let helius_url = format!(
        "https://api.helius.xyz/v0/transactions/?api-key={}",
        args.helius_api_key
    );

    let cfg = SwapsFetcherConfig::new(args.proxy_url.clone(), helius_url);
    let fetcher = SwapsFetcher::new(cfg).context("building SwapsFetcher")?;
    tracing::info!(
        pool = pool.pool_address,
        start_ts,
        end_ts,
        proxy = args.proxy_url,
        "fetching swaps"
    );
    let swaps = fetcher
        .fetch(&pool, start_ts, end_ts)
        .await
        .context("fetcher.fetch")?;
    tracing::info!(swaps = swaps.len(), "fetched; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<swap::v1::Swap>(&args.venue, Some(&pool.pool_address), &swaps)
        .context("Dataset::write")?;
    println!(
        "swaps fetched: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
