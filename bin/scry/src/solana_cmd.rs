use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::{
    mints, CollateralFilter, JupiterLendLiquidationsFetcher, JupiterLendLiquidationsFetcherConfig,
    KaminoLiquidationsFetcher, KaminoLiquidationsFetcherConfig, PoolMetadata, ReserveSymbolMap,
    SwapsFetcher, SwapsFetcherConfig,
};
use scryer_schema::{jupiter_lend_liquidation, kamino_liquidation, swap};
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

#[derive(Parser, Debug)]
pub struct KaminoLiquidationsArgs {
    /// Lending market PDA. Defaults to the xStocks market on Klend.
    #[arg(long, default_value = "5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua")]
    lending_market: String,
    /// Disables the post-decode market filter — useful for
    /// cross-Kamino-market scans (Paper 2 §C4 expansion). Sigs still
    /// come from `--lending-market`'s signature stream.
    #[arg(long, default_value_t = false)]
    all_markets: bool,
    /// Window start (`YYYY-MM-DD`, RFC 3339, or unix seconds).
    #[arg(long)]
    start: String,
    /// Window end. Same formats as `--start`.
    #[arg(long)]
    end: String,
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    #[arg(long, env = "HELIUS_API_KEY")]
    helius_api_key: String,
    /// Optional JSON map for `(reserve_pda) -> (symbol, decimals)`
    /// resolution. Shape: `{"<pda>": {"symbol": "USDC",
    /// "decimals": 6}, ...}`. Reserves not in the map decode to
    /// `("?", 0)` per the Phase-17 methodology lock.
    #[arg(long)]
    symbol_map: Option<PathBuf>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::KAMINO)]
    venue: String,
}

#[derive(Debug, Deserialize)]
struct SymbolMapEntry {
    symbol: String,
    decimals: u8,
}

pub async fn run_kamino_liquidations(args: KaminoLiquidationsArgs) -> Result<()> {
    let start_ts = crate::parse_unix_seconds(&args.start).context("parsing --start")?;
    let end_ts = crate::parse_unix_seconds(&args.end).context("parsing --end")?;
    if end_ts <= start_ts {
        anyhow::bail!("--end ({end_ts}) must be > --start ({start_ts})");
    }

    let mut symbol_map = ReserveSymbolMap::new();
    if let Some(path) = &args.symbol_map {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading symbol map {}", path.display()))?;
        let parsed: std::collections::HashMap<String, SymbolMapEntry> =
            serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing symbol map {}", path.display()))?;
        for (pda, entry) in parsed {
            symbol_map.insert(pda, entry.symbol, entry.decimals);
        }
    }

    let helius_url = format!(
        "https://api.helius.xyz/v0/transactions/?api-key={}",
        args.helius_api_key
    );
    let mut cfg = KaminoLiquidationsFetcherConfig::new(
        args.proxy_url.clone(),
        helius_url,
        args.lending_market.clone(),
    );
    if args.all_markets {
        cfg = cfg.all_markets();
    }
    let fetcher = KaminoLiquidationsFetcher::new(cfg, symbol_map).context("building KaminoLiquidationsFetcher")?;

    tracing::info!(
        market = args.lending_market,
        start_ts,
        end_ts,
        all_markets = args.all_markets,
        "fetching kamino liquidations"
    );
    let rows = fetcher.fetch(start_ts, end_ts).await.context("fetcher.fetch")?;
    tracing::info!(rows = rows.len(), "fetched; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<kamino_liquidation::v1::Liquidation>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "kamino_liquidations fetched: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct JupiterLendLiquidationsArgs {
    /// Window start (`YYYY-MM-DD`, RFC 3339, or unix seconds).
    #[arg(long)]
    start: String,
    /// Window end. Same formats as `--start`.
    #[arg(long)]
    end: String,
    /// Disables the post-decode collateral-mint filter. With this
    /// flag the panel includes liquidations on any collateral, not
    /// just the xStock mints in `--symbol-map`.
    #[arg(long, default_value_t = false)]
    all_collateral: bool,
    /// JSON map for `(mint_pubkey) -> (symbol, decimals)` resolution.
    /// In default xstock-only mode the keys also serve as the
    /// allowed-collateral set; with `--all-collateral` only the
    /// symbol/decimals lookup applies. Required unless
    /// `--all-collateral` is set.
    #[arg(long)]
    symbol_map: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    #[arg(long, env = "HELIUS_API_KEY")]
    helius_api_key: String,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::JUPITER_LEND)]
    venue: String,
}

pub async fn run_jupiter_lend_liquidations(args: JupiterLendLiquidationsArgs) -> Result<()> {
    let start_ts = crate::parse_unix_seconds(&args.start).context("parsing --start")?;
    let end_ts = crate::parse_unix_seconds(&args.end).context("parsing --end")?;
    if end_ts <= start_ts {
        anyhow::bail!("--end ({end_ts}) must be > --start ({start_ts})");
    }

    let mut symbol_map = ReserveSymbolMap::new();
    let mut allowed_mints: Vec<String> = Vec::new();
    if let Some(path) = &args.symbol_map {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading symbol map {}", path.display()))?;
        let parsed: std::collections::HashMap<String, SymbolMapEntry> =
            serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing symbol map {}", path.display()))?;
        for (mint, entry) in parsed {
            allowed_mints.push(mint.clone());
            symbol_map.insert(mint, entry.symbol, entry.decimals);
        }
    } else if !args.all_collateral {
        anyhow::bail!(
            "--symbol-map is required unless --all-collateral is set; \
             provide a JSON file mapping xstock mint pubkeys to (symbol, decimals), \
             or pass --all-collateral to disable the filter."
        );
    }

    let collateral_filter = if args.all_collateral {
        CollateralFilter::Any
    } else {
        CollateralFilter::Only(allowed_mints)
    };

    let helius_url = format!(
        "https://api.helius.xyz/v0/transactions/?api-key={}",
        args.helius_api_key
    );
    let cfg = JupiterLendLiquidationsFetcherConfig::new(
        args.proxy_url.clone(),
        helius_url,
        collateral_filter,
    );
    let fetcher = JupiterLendLiquidationsFetcher::new(cfg, symbol_map)
        .context("building JupiterLendLiquidationsFetcher")?;

    tracing::info!(
        start_ts,
        end_ts,
        all_collateral = args.all_collateral,
        "fetching jupiter lend liquidations"
    );
    let rows = fetcher.fetch(start_ts, end_ts).await.context("fetcher.fetch")?;
    tracing::info!(rows = rows.len(), "fetched; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<jupiter_lend_liquidation::v1::Liquidation>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "jupiter_lend_liquidations fetched: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
