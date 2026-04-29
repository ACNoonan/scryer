//! `scry solana chainlink-reports` — continuous Chainlink Data
//! Streams report tape on Solana.
//!
//! Walks `getSignaturesForAddress(VERIFIER_PROGRAM_ID)` over
//! `[--start, --end]`, decodes every verify CPI inner instruction,
//! and writes one `chainlink_data_streams.v1::Report` row per
//! decoded report to
//! `dataset/chainlink/data_streams/v1/year=Y/month=M/day=D.parquet`.
//!
//! Companion to the Phase 45 `cex_stock_perp_tape.v1` and Phase 59
//! `backed_nav_strikes.v1` tapes — completes the third leg of the
//! oracle-divergence analysis (issuer / Chainlink / consumer) needed
//! for paper §1.1.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::{ChainlinkReportsFetcher, ChainlinkReportsFetcherConfig};
use scryer_schema::chainlink_data_streams;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct ChainlinkReportsArgs {
    /// Window start. `YYYY-MM-DD`, RFC 3339, or unix seconds.
    #[arg(long)]
    pub start: String,
    /// Window end. Same formats as `--start`.
    #[arg(long)]
    pub end: String,
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    pub proxy_url: String,
    /// Helius API key for the parseTransactions call. Overrides
    /// `HELIUS_API_KEY` env var.
    #[arg(long, env = "HELIUS_API_KEY")]
    pub helius_api_key: String,
    /// Switch stage 2 to proxy-routed `getTransaction` instead of
    /// Helius `parseTransactions`. Slower but quota-resilient via the
    /// proxy's failover. Useful when Helius's daily quota is
    /// exhausted.
    #[arg(long, default_value_t = false)]
    pub use_get_transaction: bool,
    #[arg(long, default_value = "./dataset")]
    pub dataset: PathBuf,
    /// Venue under `dataset/`. Default `chainlink`.
    #[arg(long, default_value = scryer_store::venue::CHAINLINK)]
    pub venue: String,
}

pub async fn run_chainlink_reports(args: ChainlinkReportsArgs) -> Result<()> {
    let start_ts = crate::parse_unix_seconds(&args.start).context("parsing --start")?;
    let end_ts = crate::parse_unix_seconds(&args.end).context("parsing --end")?;
    if end_ts <= start_ts {
        anyhow::bail!("--end ({end_ts}) must be > --start ({start_ts})");
    }

    let helius_url = format!(
        "https://api.helius.xyz/v0/transactions/?api-key={}",
        args.helius_api_key
    );
    let mut cfg = ChainlinkReportsFetcherConfig::new(args.proxy_url.clone(), helius_url);
    if args.use_get_transaction {
        cfg.use_get_transaction = true;
        cfg.source_label = "rpc:getTransaction".to_string();
    }
    let fetcher =
        ChainlinkReportsFetcher::new(cfg).context("building ChainlinkReportsFetcher")?;

    tracing::info!(
        start_ts,
        end_ts,
        proxy = args.proxy_url,
        use_get_transaction = args.use_get_transaction,
        "fetching Chainlink Data Streams reports"
    );
    let rows = fetcher.fetch(start_ts, end_ts).await.context("fetcher.fetch")?;
    tracing::info!(rows = rows.len(), "decoded; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<chainlink_data_streams::v1::Report>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "chainlink_data_streams fetched: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
