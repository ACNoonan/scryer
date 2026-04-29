//! `scry rss backed` + `scry rss nasdaq-halts` — RSS / Atom-feed
//! fetchers. Single-tick mode; cadence wrapped externally by launchd
//! / cron.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_rss::{
    backed_corp_actions::{self, commits_atom_url, DEFAULT_BRANCH, DEFAULT_REPO},
    fetch_body, nasdaq_halts, wayback, PollConfig,
};
use scryer_schema::{backed, nasdaq_halts as schema_halts, Meta};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct BackedArgs {
    /// GitHub repo slug for the corp-actions feed.
    #[arg(long, default_value = DEFAULT_REPO)]
    repo: String,
    /// Branch name in the repo.
    #[arg(long, default_value = DEFAULT_BRANCH)]
    branch: String,
    /// Override the full Atom feed URL (overrides --repo/--branch when set).
    #[arg(long)]
    feed_url: Option<String>,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "github:atom")]
    source: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::BACKED)]
    venue: String,
}

#[derive(Parser, Debug)]
pub struct NasdaqHaltsArgs {
    /// Override the Nasdaq trade-halts RSS URL.
    #[arg(long, default_value = nasdaq_halts::DEFAULT_FEED_URL)]
    feed_url: String,
    /// `_source` stamped on every emitted row in live-mode. Backfill
    /// mode overrides this per-snapshot to `nasdaq:wayback:{ts}` so
    /// consumers can audit which Wayback crawl emitted each row.
    #[arg(long, default_value = "nasdaq:rss")]
    source: String,
    /// Backfill window start as `YYYY-MM-DD` UTC. When set, the CLI
    /// switches from live single-tick mode to **historical-backfill
    /// mode** — it queries the Internet Archive Wayback Machine's
    /// CDX index for snapshots of `feed_url` in `[backfill,
    /// backfill_end]`, fetches each archived RSS, and writes the
    /// decoded halts. Coverage is partial — see item 15b in
    /// `wishlist.md` and `wayback.rs` "Coverage caveat" for the
    /// gap-disclosure semantics. The standard live-mode path is
    /// unchanged when this flag is omitted.
    #[arg(long)]
    backfill: Option<String>,
    /// Backfill window end as `YYYY-MM-DD` UTC. Defaults to today.
    /// Ignored unless `--backfill` is set.
    #[arg(long)]
    backfill_end: Option<String>,
    /// Delay between successive Wayback fetches in milliseconds. The
    /// Wayback Machine has its own per-IP throttle; default 1500ms is
    /// a polite cadence for a one-shot backfill.
    #[arg(long, default_value_t = 1500)]
    backfill_rate_limit_ms: u64,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::NASDAQ)]
    venue: String,
}

pub async fn run_backed(args: BackedArgs) -> Result<()> {
    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        source_label: args.source.clone(),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;

    let url = args
        .feed_url
        .clone()
        .unwrap_or_else(|| commits_atom_url(&args.repo, &args.branch));
    let now = Utc::now();
    let detected_at = now.timestamp_micros();
    let meta = Meta::new(backed::v1::SCHEMA_VERSION, now.timestamp(), &args.source);

    tracing::info!(repo = args.repo, url = url, "fetching Backed corp-actions feed");
    let body = fetch_body(&client, &url, &cfg).await.context("fetch_body")?;
    let rows = backed_corp_actions::parse_feed(&body, &args.repo, detected_at, &meta)
        .context("parse_feed")?;
    tracing::info!(rows = rows.len(), "decoded");

    if rows.is_empty() {
        println!("rss backed: rows_added=0 rows_deduped=0 partitions_written=0 (empty feed)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<backed::v1::Action>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "rss backed: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

pub async fn run_nasdaq_halts(args: NasdaqHaltsArgs) -> Result<()> {
    if args.backfill.is_some() {
        return run_nasdaq_halts_backfill(args).await;
    }
    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        source_label: args.source.clone(),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let poll_unix_micros = now.timestamp_micros();
    let meta = Meta::new(schema_halts::v1::SCHEMA_VERSION, now.timestamp(), &args.source);

    tracing::info!(url = args.feed_url, "fetching Nasdaq trade-halts RSS");
    let body = fetch_body(&client, &args.feed_url, &cfg).await.context("fetch_body")?;
    let rows = nasdaq_halts::parse_feed(&body, poll_unix_micros, &meta).context("parse_feed")?;
    tracing::info!(rows = rows.len(), "decoded");

    if rows.is_empty() {
        println!(
            "rss nasdaq-halts: rows_added=0 rows_deduped=0 partitions_written=0 (empty feed)"
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<schema_halts::v1::Halt>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "rss nasdaq-halts: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}

/// Wayback-backed historical backfill of `nasdaq_halts.v1`.
///
/// **Coverage is partial** — Wayback's crawl cadence on the trade-
/// halts RSS is sparse (typically 1-3 snapshots / quarter). Each
/// snapshot captures only the halts active at the crawl moment, so
/// halts that opened-and-closed entirely between two crawls are
/// missed. See `wayback.rs` "Coverage caveat" + wishlist item 15b.
async fn run_nasdaq_halts_backfill(args: NasdaqHaltsArgs) -> Result<()> {
    let backfill_start = args
        .backfill
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--backfill is required for backfill mode"))?;
    let backfill_end = args
        .backfill_end
        .clone()
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string());

    let from = ymd_to_yyyymmdd(backfill_start)?;
    let to = ymd_to_yyyymmdd(&backfill_end)?;

    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        source_label: args.source.clone(),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;

    tracing::info!(
        feed_url = args.feed_url,
        from = %from,
        to = %to,
        "querying Wayback CDX for trade-halts snapshots"
    );
    let snapshots = wayback::list_snapshots(&client, &cfg, &args.feed_url, &from, &to)
        .await
        .context("wayback::list_snapshots")?;
    tracing::info!(snapshots = snapshots.len(), "decoded CDX index");
    if snapshots.is_empty() {
        println!(
            "rss nasdaq-halts backfill: snapshots=0 rows_added=0 rows_deduped=0 partitions_written=0"
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    let mut snapshots_failed = 0usize;
    let mut total_rows_decoded = 0usize;

    for (idx, snap) in snapshots.iter().enumerate() {
        let url = snap.fetch_url();
        let snapshot_unix = snap.timestamp_unix().unwrap_or(0);
        let body = match fetch_body(&client, &url, &cfg).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(timestamp = %snap.timestamp, error = %e, "wayback fetch failed; skipping");
                snapshots_failed += 1;
                if args.backfill_rate_limit_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(args.backfill_rate_limit_ms)).await;
                }
                continue;
            }
        };

        // Stamp `_source` with the snapshot timestamp so consumers can
        // audit which crawl produced each row + identify coverage gaps
        // by `SELECT DISTINCT _source` at query time.
        let source = format!("nasdaq:wayback:{}", snap.timestamp);
        let meta = Meta::new(
            schema_halts::v1::SCHEMA_VERSION,
            snapshot_unix.max(0),
            &source,
        );
        // poll_ts is the snapshot timestamp in microseconds (the
        // moment Wayback crawled the RSS — the closest analog to the
        // live-feed's poll_ts semantics).
        let poll_unix_micros = snapshot_unix.saturating_mul(1_000_000);
        let rows = match nasdaq_halts::parse_feed(&body, poll_unix_micros, &meta) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(timestamp = %snap.timestamp, error = %e, "wayback parse failed; skipping");
                snapshots_failed += 1;
                if args.backfill_rate_limit_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(args.backfill_rate_limit_ms)).await;
                }
                continue;
            }
        };
        total_rows_decoded += rows.len();
        tracing::info!(
            idx = idx + 1,
            of = snapshots.len(),
            timestamp = %snap.timestamp,
            rows = rows.len(),
            "decoded snapshot"
        );

        if !rows.is_empty() {
            let stats = ds
                .write::<schema_halts::v1::Halt>(&args.venue, None, &rows)
                .with_context(|| format!("Dataset::write @ snapshot {}", snap.timestamp))?;
            total_added += stats.rows_added;
            total_deduped += stats.rows_deduped;
            total_partitions += stats.partitions_written;
        }

        if args.backfill_rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.backfill_rate_limit_ms)).await;
        }
    }

    println!(
        "rss nasdaq-halts backfill: snapshots={} snapshots_failed={} rows_decoded={} rows_added={} rows_deduped={} partitions_written={}",
        snapshots.len(),
        snapshots_failed,
        total_rows_decoded,
        total_added,
        total_deduped,
        total_partitions
    );
    Ok(())
}

/// `YYYY-MM-DD` UTC → Wayback's `YYYYMMDD` query format.
fn ymd_to_yyyymmdd(ymd: &str) -> Result<String> {
    let d = chrono::NaiveDate::parse_from_str(ymd, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {ymd}"))?;
    Ok(d.format("%Y%m%d").to_string())
}
