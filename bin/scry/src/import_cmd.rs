use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_store::import::{
    read_legacy_kamino_scope_parquet, read_legacy_swap_parquet, read_legacy_trade_parquet,
    ImportOptions,
};
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct SwapsArgs {
    /// Path to the existing parquet file.
    #[arg(long)]
    input: PathBuf,
    /// Venue string. Examples: "solana_raydium_v4", "solana_raydium_clmm".
    #[arg(long)]
    venue: String,
    /// Pool key (Solana base58 address).
    #[arg(long)]
    pool: String,
    /// `_source` label stamped on imported rows. Defaults to
    /// `import:legacy:<filename>`.
    #[arg(long)]
    source: Option<String>,
    /// Output dataset root.
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

#[derive(Parser, Debug)]
pub struct TradesArgs {
    #[arg(long)]
    input: PathBuf,
    /// Venue string. Examples: "kraken", "hyperliquid".
    #[arg(long)]
    venue: String,
    /// Pair key (e.g. `XSOLZUSD` for Kraken SOL/USD).
    #[arg(long)]
    pair: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

fn default_source(input: &std::path::Path) -> String {
    let name = input
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!("import:legacy:{name}")
}

pub async fn run_swaps(args: SwapsArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy swap parquet"
    );
    let rows = read_legacy_swap_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write_swaps(&args.venue, &args.pool, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "swaps imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct KaminoScopeArgs {
    #[arg(long)]
    input: PathBuf,
    /// Venue string under `dataset/`. Defaults to `kamino_scope`
    /// (the methodology-locked convention).
    #[arg(long, default_value = scryer_store::venue::KAMINO_SCOPE)]
    venue: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_kamino_scope(args: KaminoScopeArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy kamino_scope parquet"
    );
    let rows = read_legacy_kamino_scope_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write_kamino_scope(&args.venue, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "kamino_scope imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

pub async fn run_trades(args: TradesArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy trade parquet"
    );
    let rows = read_legacy_trade_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write_trades(&args.venue, &args.pair, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "trades imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
