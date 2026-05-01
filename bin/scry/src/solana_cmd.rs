use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::{
    canonical_xstock_chain_map, mints, poll_kamino_scope_once, CollateralFilter,
    FluidVaultConfigsFetcher, FluidVaultConfigsFetcherConfig, JupiterLendLiquidationsFetcher,
    JupiterLendLiquidationsFetcherConfig, KaminoLiquidationsFetcher,
    KaminoLiquidationsFetcherConfig, PoolMetadata, ReserveSymbolMap, SupplyMintFilter,
    SwapsFetcher, SwapsFetcherConfig, SCOPE_PDA,
};
use scryer_schema::{
    fluid_vault_config, jupiter_lend_liquidation, kamino_liquidation, kamino_scope, swap,
};
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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    /// Venue string under `dataset/`. Defaults to
    /// `scryer_store::venue::SOLANA_RAYDIUM_V4`.
    #[arg(long, default_value = scryer_store::venue::SOLANA_RAYDIUM_V4)]
    venue: String,
    /// Hard cap on stage-1 `getSignaturesForAddress` pages walked
    /// before the fetcher bails. The pagination starts at "now" and
    /// walks backward, so the cap is on TOTAL pages (1000 sigs each)
    /// regardless of where `--start` falls. Default 5000 (= 5M sigs)
    /// — safe ceiling for ≤30d windows on most pools. For longer
    /// windows on high-volume pools (e.g. Raydium SOL/USDC over 180d),
    /// pass a higher value: 30000 (= 30M sigs) covers any reasonable
    /// single-pool scan.
    #[arg(long, default_value_t = 5_000)]
    max_sig_pages: u32,
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

    let mut cfg = SwapsFetcherConfig::new(args.proxy_url.clone(), helius_url);
    cfg.paginate.max_pages = args.max_sig_pages;
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
    /// Use proxy-routed `getTransaction` for stage 2 instead of
    /// Helius `parseTransactions`. Slower (~5-50 tx/s vs ~100 tx/s)
    /// but multi-provider quota-resilient via the proxy. Use this
    /// when Helius's daily quota is exhausted or you want full
    /// proxy routing.
    #[arg(long, default_value_t = false)]
    use_get_transaction: bool,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
    if args.use_get_transaction {
        cfg.use_get_transaction = true;
        cfg.source_label = "rpc:getTransaction".to_string();
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
    /// Same semantics as `kamino-liquidations`: use proxy-routed
    /// `getTransaction` for stage 2 instead of Helius
    /// `parseTransactions`.
    #[arg(long, default_value_t = false)]
    use_get_transaction: bool,
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    #[arg(long, env = "HELIUS_API_KEY")]
    helius_api_key: String,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
    let mut cfg = JupiterLendLiquidationsFetcherConfig::new(
        args.proxy_url.clone(),
        helius_url,
        collateral_filter,
    );
    if args.use_get_transaction {
        cfg.use_get_transaction = true;
        cfg.source_label = "rpc:getTransaction".to_string();
    }
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

#[derive(Parser, Debug)]
pub struct FluidVaultConfigsArgs {
    /// Disables the post-decode supply-mint filter. With this flag the
    /// snapshot includes every VaultConfig program-wide; default mode
    /// keeps only configs whose `supply_token` is in `--symbol-map`'s
    /// keys.
    #[arg(long, default_value_t = false)]
    all: bool,
    /// JSON map for `(mint_pubkey) -> (symbol, decimals)`. Required
    /// unless `--all` is set; in default xstock-only mode the keys
    /// also serve as the allowed-supply-mint set.
    #[arg(long)]
    symbol_map: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::JUPITER_LEND)]
    venue: String,
}

pub async fn run_fluid_vault_configs(args: FluidVaultConfigsArgs) -> Result<()> {
    use std::collections::HashSet;

    let mut symbol_map = ReserveSymbolMap::new();
    let mut allowed_mints: HashSet<String> = HashSet::new();
    if let Some(path) = &args.symbol_map {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading symbol map {}", path.display()))?;
        let parsed: std::collections::HashMap<String, SymbolMapEntry> =
            serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing symbol map {}", path.display()))?;
        for (mint, entry) in parsed {
            allowed_mints.insert(mint.clone());
            symbol_map.insert(mint, entry.symbol, entry.decimals);
        }
    } else if !args.all {
        anyhow::bail!(
            "--symbol-map is required unless --all is set; provide a JSON file \
             mapping xstock supply-mint pubkeys to (symbol, decimals), or pass \
             --all to disable the filter."
        );
    }

    let supply_filter = if args.all {
        SupplyMintFilter::Any
    } else {
        SupplyMintFilter::Only(allowed_mints)
    };

    let cfg = FluidVaultConfigsFetcherConfig::new(args.proxy_url.clone(), supply_filter);
    let fetcher = FluidVaultConfigsFetcher::new(cfg, symbol_map)
        .context("building FluidVaultConfigsFetcher")?;

    tracing::info!(all = args.all, "snapshotting fluid vault configs");
    let rows = fetcher.fetch().await.context("fetcher.fetch")?;
    tracing::info!(rows = rows.len(), "fetched; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<fluid_vault_config::v1::Config>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "fluid_vault_configs fetched: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct KaminoScopeTapeArgs {
    /// Single-tick mode. Currently the only supported mode; future
    /// `--daemon` (with internal 60s ticker) will land if cron /
    /// launchd cadence proves insufficient.
    #[arg(long, default_value_t = true)]
    once: bool,
    /// Optional JSON map for `{symbol: chain_id}` overriding the
    /// hardcoded canonical xStock map. Useful when Kamino governance
    /// rewires a reserve's Scope chain index.
    #[arg(long)]
    chain_map: Option<PathBuf>,
    /// Override the OraclePrices PDA. Defaults to Kamino's shared
    /// xStock feed `3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH`.
    #[arg(long, default_value = SCOPE_PDA)]
    feed_pda: String,
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    #[arg(long, default_value = "kamino:scope-tape")]
    source: String,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::KAMINO_SCOPE)]
    venue: String,
}

pub async fn run_kamino_scope_tape(args: KaminoScopeTapeArgs) -> Result<()> {
    use std::collections::HashMap;

    let chain_map: HashMap<String, u32> = if let Some(path) = &args.chain_map {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading chain map {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing chain map {}", path.display()))?
    } else {
        canonical_xstock_chain_map()
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    tracing::info!(
        feed_pda = args.feed_pda,
        symbols = chain_map.len(),
        proxy = args.proxy_url,
        "polling Kamino-Scope tape"
    );
    let rows =
        poll_kamino_scope_once(&client, &args.proxy_url, &args.feed_pda, &chain_map, &args.source)
            .await
            .context("poll_once_via_proxy")?;
    tracing::info!(rows = rows.len(), "decoded; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<kamino_scope::v1::Reading>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "kamino_scope_tape polled: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
