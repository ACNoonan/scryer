//! `scry solana chainlink-reports` — continuous Chainlink Data
//! Streams report tape on Solana.
//!
//! Walks `getSignaturesForAddress(VERIFIER_PROGRAM_ID)` over a window,
//! decodes every verify CPI inner instruction, and writes one
//! `chainlink_data_streams.v1::Report` row per decoded report to
//! `dataset/chainlink/data_streams/v1/year=Y/month=M/day=D.parquet`.
//!
//! Two driving modes:
//! - **Range** (`--start --end`): backfill walker for historical depth.
//! - **Single-tick** (`--once --lookback-secs N`): forward-poll mode
//!   driven externally by launchd / cron at the desired cadence
//!   (typical: 60s tick, 120s lookback for jitter resilience). Mirrors
//!   the v5-tape and Kamino-/Jupiter-Lend liquidation conventions.
//!
//! Companion to the Phase 45 `cex_stock_perp_tape.v1` and Phase 59
//! `backed_nav_strikes.v1` tapes — completes the third leg of the
//! oracle-divergence analysis (issuer / Chainlink / consumer) needed
//! for paper §1.1.

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_solana::{ChainlinkReportsFetcher, ChainlinkReportsFetcherConfig};
use scryer_schema::chainlink_data_streams;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct ChainlinkReportsArgs {
    /// Single-tick mode. Computes `[now - lookback_secs, now]` and
    /// fetches that window. Cadence is driven externally by launchd /
    /// cron at the desired interval (typical: 60s). Mutually exclusive
    /// with `--start`/`--end`.
    #[arg(long, default_value_t = false)]
    pub once: bool,
    /// Lookback window (seconds) used by `--once`. Default 120 mirrors
    /// the v5-tape convention — long enough to absorb jitter on the
    /// 60s plist tick without burning quota on duplicate decodes
    /// (writer dedups on `chainlink:{feed_id}:{observation_ts}:{sig}`).
    #[arg(long, default_value_t = 120)]
    pub lookback_secs: i64,
    /// Window start. `YYYY-MM-DD`, RFC 3339, or unix seconds. Required
    /// in range mode (`--start` + `--end`); ignored when `--once` is set.
    #[arg(long)]
    pub start: Option<String>,
    /// Window end. Same formats as `--start`.
    #[arg(long)]
    pub end: Option<String>,
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
    /// exhausted; recommended default for the launchd daemon.
    #[arg(long, default_value_t = false)]
    pub use_get_transaction: bool,
    /// `_source` stamped on every emitted row. Defaults to
    /// `chainlink:data-streams`. Launchd plist passes
    /// `chainlink:data-streams:launchd` to distinguish forward-poll
    /// rows from backfill rows.
    #[arg(long, default_value = "chainlink:data-streams")]
    pub source: String,
    #[arg(long, default_value = "./dataset")]
    pub dataset: PathBuf,
    /// Venue under `dataset/`. Default `chainlink`.
    #[arg(long, default_value = scryer_store::venue::CHAINLINK)]
    pub venue: String,
}

pub async fn run_chainlink_reports(args: ChainlinkReportsArgs) -> Result<()> {
    let (start_ts, end_ts) = if args.once {
        if args.start.is_some() || args.end.is_some() {
            anyhow::bail!("--once is mutually exclusive with --start / --end");
        }
        if args.lookback_secs <= 0 {
            anyhow::bail!("--lookback-secs must be > 0");
        }
        let now = Utc::now().timestamp();
        (now - args.lookback_secs, now)
    } else {
        let start_raw = args.start.as_deref().ok_or_else(|| {
            anyhow::anyhow!("--start required in range mode (or pass --once --lookback-secs N)")
        })?;
        let end_raw = args.end.as_deref().ok_or_else(|| {
            anyhow::anyhow!("--end required in range mode (or pass --once --lookback-secs N)")
        })?;
        let start_ts = crate::parse_unix_seconds(start_raw).context("parsing --start")?;
        let end_ts = crate::parse_unix_seconds(end_raw).context("parsing --end")?;
        if end_ts <= start_ts {
            anyhow::bail!("--end ({end_ts}) must be > --start ({start_ts})");
        }
        (start_ts, end_ts)
    };

    let helius_url = format!(
        "https://api.helius.xyz/v0/transactions/?api-key={}",
        args.helius_api_key
    );
    let mut cfg = ChainlinkReportsFetcherConfig::new(args.proxy_url.clone(), helius_url);
    cfg.source_label = args.source.clone();
    if args.use_get_transaction {
        cfg.use_get_transaction = true;
    }
    let fetcher =
        ChainlinkReportsFetcher::new(cfg).context("building ChainlinkReportsFetcher")?;

    tracing::info!(
        once = args.once,
        start_ts,
        end_ts,
        proxy = args.proxy_url,
        use_get_transaction = args.use_get_transaction,
        source = args.source,
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
