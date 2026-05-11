//! `scry pyth-lazer subscribe` — connect to the Pyth Lazer WebSocket,
//! drain price-update messages for the configured duration, and write
//! `oracle.pyth_lazer.tape.v2::Row` rows to parquet.
//!
//! Methodology lock: `methodology_log.md` "Pyth Lazer ingestion —
//! 2026-05-10". Wishlist row: "Pyth Lazer fetcher" (added 2026-05-10).
//!
//! `--dry-run` mode skips the parquet write and prints a per-symbol
//! summary; useful for the first-fire probe answering whether the
//! free tier accepts equity-feed subscriptions.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_pyth_lazer::{
    run_subscribe, LazerChannel, PollConfig, DEFAULT_CRYPTO_SYMBOLS, DEFAULT_EQUITY_SYMBOLS,
    DEFAULT_XSTOCK_SYMBOLS,
};
use scryer_schema::oracle_pyth_lazer_tape::v2::Row;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct SubscribeArgs {
    /// Pyth Lazer access token. Default: read from `LAZER_ACCESS_TOKEN`
    /// env var, falling back to `PYTH_LAZER_API_KEY` for parity with
    /// the deployed `.env` naming convention.
    #[arg(long, env = "LAZER_ACCESS_TOKEN")]
    access_token: Option<String>,
    /// Comma-separated Pyth-canonical symbol strings. When omitted,
    /// defaults to the union of the crypto control set + equity-underlier
    /// panel + xStock direct-token panel hardcoded in the fetcher
    /// (~26 symbols total).
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Cadence channel. Allowed: `real_time`, `fixed_rate@50ms`,
    /// `fixed_rate@200ms`, `fixed_rate@1000ms`. Default 200ms is the
    /// steady-cadence pick — 50ms is unnecessarily fine for tape
    /// capture; real_time is unthrottled and floods.
    #[arg(long, default_value = "fixed_rate@200ms")]
    channel: String,
    /// How long to keep the subscription open before exiting. Default
    /// 45s is for one-shot operator probes. The runner manifest pins
    /// `--duration-secs=30` because each per-feed parquet write adds
    /// ~10s of merge-dedup at end-of-UTC-day partition sizes, and
    /// total wall-clock must stay under launchd's 60s `StartInterval`
    /// boundary to avoid skip-if-running halving the cadence to 120s.
    #[arg(long, default_value_t = 45)]
    duration_secs: u64,
    /// Number of redundant WS connections (SDK default 4). Lower is
    /// fine for the cycling-fire pattern.
    #[arg(long, default_value_t = 2)]
    num_connections: usize,
    /// Skip the parquet write and print a per-symbol summary instead.
    /// Use for the first-fire probe to confirm the free tier accepts
    /// equity-feed subscriptions before committing to a runner cadence.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "pyth-lazer:ws")]
    source: String,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::ORACLE_PYTH_LAZER)]
    venue: String,
}

pub async fn run_subscribe_cmd(args: SubscribeArgs) -> Result<()> {
    // Resolve the access token: prefer the explicit --access-token,
    // then LAZER_ACCESS_TOKEN (handled by clap's env), then fall back
    // to PYTH_LAZER_API_KEY which is the deployed .env naming.
    let token = args
        .access_token
        .clone()
        .or_else(|| std::env::var("PYTH_LAZER_API_KEY").ok())
        .filter(|t| !t.is_empty())
        .context(
            "Pyth Lazer access token required: pass --access-token, set LAZER_ACCESS_TOKEN, or set PYTH_LAZER_API_KEY in the env",
        )?;

    let channel: LazerChannel = args.channel.parse().map_err(|e: String| anyhow::anyhow!(e))?;

    let symbols: Vec<String> = if args.symbols.is_empty() {
        DEFAULT_CRYPTO_SYMBOLS
            .iter()
            .chain(DEFAULT_EQUITY_SYMBOLS.iter())
            .chain(DEFAULT_XSTOCK_SYMBOLS.iter())
            .map(|s| s.to_string())
            .collect()
    } else {
        args.symbols.clone()
    };

    let cfg = PollConfig {
        access_token: token,
        endpoints: Vec::new(),
        num_connections: args.num_connections,
        symbols: symbols.clone(),
        channel,
        duration: Duration::from_secs(args.duration_secs),
        source_label: args.source.clone(),
        connect_timeout: Duration::from_secs(10),
    };

    tracing::info!(
        symbols = symbols.len(),
        channel = cfg.channel.as_str(),
        duration_secs = args.duration_secs,
        dry_run = args.dry_run,
        "starting Pyth Lazer subscribe cycle"
    );

    let (rows, stats) = run_subscribe(&cfg).await.context("run_subscribe")?;

    // Per-feed summary regardless of dry-run, since it's load-bearing
    // for the first-fire probe.
    let mut by_feed: BTreeMap<u32, usize> = BTreeMap::new();
    for r in &rows {
        *by_feed.entry(r.price_feed_id).or_insert(0) += 1;
    }

    println!(
        "pyth-lazer subscribe: messages={} rows={} feeds_seen={} elapsed_s={:.1}",
        stats.messages_received,
        stats.rows_emitted,
        stats.feeds_seen,
        stats.elapsed.as_secs_f64()
    );
    println!("per-feed row counts (top 20):");
    for (feed_id, count) in by_feed.iter().take(20) {
        println!("  feed_id={feed_id} rows={count}");
    }

    if args.dry_run {
        println!("[dry-run] skipping parquet write");
        return Ok(());
    }

    if rows.is_empty() {
        println!("pyth-lazer subscribe: no rows to write");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    // Group rows by `price_feed_id` partition before writing — the
    // DatasetSchema impl declares PARTITION_KEY_PREFIX = Some("feed_id")
    // (canonical Lazer identifier; / -free, unlike the symbol string).
    let mut by_partition: BTreeMap<u32, Vec<Row>> = BTreeMap::new();
    for row in rows {
        by_partition.entry(row.price_feed_id).or_default().push(row);
    }

    let mut total_added = 0;
    let mut total_deduped = 0;
    let mut total_partitions = 0;
    for (feed_id, batch) in by_partition {
        let key = feed_id.to_string();
        let stats = ds
            .write::<Row>(&args.venue, Some(&key), &batch)
            .with_context(|| format!("Dataset::write oracle_pyth_lazer_tape for feed_id={feed_id}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }

    println!(
        "pyth-lazer subscribe: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions}"
    );
    Ok(())
}
