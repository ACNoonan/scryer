use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_schema::{
    backed, earnings, kamino_scope, nasdaq_halts, pyth, redstone, swap, trade, v5_tape, yahoo,
};
use scryer_store::import::{
    read_legacy_backed_parquet, read_legacy_earnings_parquet, read_legacy_kamino_scope_parquet,
    read_legacy_nasdaq_halts_parquet, read_legacy_pyth_parquet, read_legacy_redstone_parquet,
    read_legacy_swap_parquet, read_legacy_trade_parquet, read_legacy_v5_tape_parquet,
    read_legacy_yahoo_parquet, ImportOptions,
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
        .write::<swap::v1::Swap>(&args.venue, Some(&args.pool), &rows)
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
        .write::<kamino_scope::v1::Reading>(&args.venue, None, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "kamino_scope imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct PythArgs {
    #[arg(long)]
    input: PathBuf,
    /// Venue string under `dataset/`. Defaults to `pyth`.
    #[arg(long, default_value = scryer_store::venue::PYTH)]
    venue: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_pyth(args: PythArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy pyth parquet"
    );
    let rows = read_legacy_pyth_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<pyth::v1::Reading>(&args.venue, None, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "pyth imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct V5TapeArgs {
    #[arg(long)]
    input: PathBuf,
    /// Venue string under `dataset/`. Defaults to `soothsayer_v5`
    /// per the methodology log "Soothsayer venue versioning" rule.
    #[arg(long, default_value = scryer_store::venue::SOOTHSAYER_V5)]
    venue: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_v5_tape(args: V5TapeArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy v5_tape parquet"
    );
    let rows = read_legacy_v5_tape_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<v5_tape::v1::Reading>(&args.venue, None, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "v5_tape imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct RedstoneArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long, default_value = scryer_store::venue::REDSTONE)]
    venue: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_redstone(args: RedstoneArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy redstone parquet"
    );
    let rows = read_legacy_redstone_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<redstone::v1::Reading>(&args.venue, None, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "redstone imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct YahooArgs {
    /// One or more parquet paths. Shell glob expansion works:
    /// `--input data/raw/yahoo_*.parquet`. All input files merge
    /// into the same `dataset/yahoo/equities_daily/v1/...` tree
    /// with dedup by `(symbol, ts)`.
    #[arg(long, num_args = 1.., required = true)]
    input: Vec<PathBuf>,
    #[arg(long, default_value = scryer_store::venue::YAHOO)]
    venue: String,
    /// `_source` label stamped on imported rows. Defaults to
    /// `import:legacy:yahoo` (uniform across all merged files).
    #[arg(long, default_value = "import:legacy:yahoo")]
    source: String,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_yahoo(args: YahooArgs) -> Result<()> {
    use std::collections::BTreeMap;

    let ds = Dataset::new(&args.dataset);
    let mut total_rows_loaded = 0usize;
    // Concatenate all rows across input files first; the per-file
    // mtime that ImportOptions::from_file_mtime would emit doesn't
    // matter here because all rows get the same `_source` label and
    // dedup-on-(symbol, ts) collapses the heavy overlap between
    // overlapping cache files. _fetched_at = first-input mtime.
    let first_path = args.input.first().expect("clap requires at least one --input");
    let opts = ImportOptions::from_file_mtime(first_path, &args.source)
        .with_context(|| format!("reading mtime of {}", first_path.display()))?;
    tracing::info!(
        files = args.input.len(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading yahoo parquet files"
    );
    let mut all_rows: Vec<yahoo::v1::Bar> = Vec::new();
    for input in &args.input {
        let rows = read_legacy_yahoo_parquet(input, &opts)
            .with_context(|| format!("reading {}", input.display()))?;
        total_rows_loaded += rows.len();
        all_rows.extend(rows);
    }
    tracing::info!(rows = all_rows.len(), "loaded; bucketing by symbol");

    // Yahoo's partition key (`symbol`) is intrinsic to each row, not
    // constant per write call. Bucket by symbol first; one Dataset::write
    // call per symbol.
    let mut by_symbol: BTreeMap<String, Vec<yahoo::v1::Bar>> = BTreeMap::new();
    for r in all_rows {
        by_symbol.entry(r.symbol.clone()).or_default().push(r);
    }
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, rows) in &by_symbol {
        let stats = ds
            .write::<yahoo::v1::Bar>(&args.venue, Some(symbol), rows)
            .with_context(|| format!("writing {} rows for symbol={}", rows.len(), symbol))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "yahoo imported: files={} rows_loaded={} symbols={} rows_added={} rows_deduped={} partitions_written={}",
        args.input.len(),
        total_rows_loaded,
        by_symbol.len(),
        total_added,
        total_deduped,
        total_partitions
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct EarningsArgs {
    /// One or more parquet paths. Shell glob expansion works:
    /// `--input data/raw/earnings_*.parquet`. All input files merge
    /// into the same `dataset/yahoo/earnings/v1/...` tree with dedup
    /// by `(symbol, earnings_date)`.
    #[arg(long, num_args = 1.., required = true)]
    input: Vec<PathBuf>,
    /// Venue. Defaults to `yahoo` since the data comes from yfinance.
    #[arg(long, default_value = scryer_store::venue::YAHOO)]
    venue: String,
    #[arg(long, default_value = "import:legacy:earnings")]
    source: String,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_earnings(args: EarningsArgs) -> Result<()> {
    use std::collections::BTreeMap;

    let ds = Dataset::new(&args.dataset);
    let first_path = args.input.first().expect("clap requires at least one --input");
    let opts = ImportOptions::from_file_mtime(first_path, &args.source)
        .with_context(|| format!("reading mtime of {}", first_path.display()))?;
    tracing::info!(
        files = args.input.len(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading earnings parquet files"
    );
    let mut all_rows: Vec<earnings::v1::Event> = Vec::new();
    let mut total_rows_loaded = 0usize;
    for input in &args.input {
        let rows = read_legacy_earnings_parquet(input, &opts)
            .with_context(|| format!("reading {}", input.display()))?;
        total_rows_loaded += rows.len();
        all_rows.extend(rows);
    }

    // Same per-row partition key pattern as yahoo: bucket by symbol.
    let mut by_symbol: BTreeMap<String, Vec<earnings::v1::Event>> = BTreeMap::new();
    for r in all_rows {
        by_symbol.entry(r.symbol.clone()).or_default().push(r);
    }
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, rows) in &by_symbol {
        let stats = ds
            .write::<earnings::v1::Event>(&args.venue, Some(symbol), rows)
            .with_context(|| format!("writing {} rows for symbol={}", rows.len(), symbol))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "earnings imported: files={} rows_loaded={} symbols={} rows_added={} rows_deduped={} partitions_written={}",
        args.input.len(),
        total_rows_loaded,
        by_symbol.len(),
        total_added,
        total_deduped,
        total_partitions
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct BackedArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long, default_value = scryer_store::venue::BACKED)]
    venue: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_backed(args: BackedArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy backed corp-actions parquet"
    );
    let rows = read_legacy_backed_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<backed::v1::Action>(&args.venue, None, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "backed imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct NasdaqHaltsArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long, default_value = scryer_store::venue::NASDAQ)]
    venue: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_nasdaq_halts(args: NasdaqHaltsArgs) -> Result<()> {
    let label = args.source.clone().unwrap_or_else(|| default_source(&args.input));
    let opts = ImportOptions::from_file_mtime(&args.input, &label)
        .with_context(|| format!("reading mtime of {}", args.input.display()))?;
    tracing::info!(
        path = %args.input.display(),
        source = %opts.source_label,
        fetched_at = opts.fetched_at,
        "loading legacy nasdaq_halts parquet"
    );
    let rows = read_legacy_nasdaq_halts_parquet(&args.input, &opts)
        .with_context(|| format!("reading {}", args.input.display()))?;
    tracing::info!(rows = rows.len(), "loaded; writing to dataset");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<nasdaq_halts::v1::Halt>(&args.venue, None, &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "nasdaq_halts imported: rows_added={} rows_deduped={} partitions_written={}",
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
        .write::<trade::v1::Trade>(&args.venue, Some(&args.pair), &rows)
        .with_context(|| format!("writing to {}", args.dataset.display()))?;
    println!(
        "trades imported: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
