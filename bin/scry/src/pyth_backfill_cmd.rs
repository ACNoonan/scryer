//! `scry pyth backfill` — historical xStock Pyth tape via the Pyth
//! Benchmarks API.
//!
//! Iterates over `[--start, --end)` at minute boundaries; for each
//! minute boundary `T`, calls
//! `benchmarks.pyth.network/v1/updates/price/{T-60}/60?ids=...&parsed=true`
//! with all 32 xStock feed IDs in one batch, picks the latest publish
//! per feed in the window, and writes one `pyth.v1::Reading` row per
//! feed-with-data-in-bucket. Off-hours session feeds (e.g. SPY-on
//! during US cash hours) emit no row; outer-join on the consumer side.
//!
//! See `methodology_log.md` "Pyth Benchmarks historical backfill —
//! 2026-05-01 (locked)" for the API audit + bucket-alignment design.
//!
//! Run pattern (from a docs example):
//!
//! ```text
//! scry pyth backfill --start 2026-02-01 --end 2026-04-30 \
//!   --rate-limit-ms 100 --source pyth:hermes:benchmarks
//! ```

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use scryer_fetch_pyth::{
    default_feed_refs, poll_window, BackfillConfig, FeedRef, DEFAULT_BENCHMARKS_URL_BASE,
};
use scryer_schema::pyth;
use scryer_schema::Meta;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct BackfillArgs {
    /// Window start. `YYYY-MM-DD`, RFC 3339, or unix seconds.
    /// Aligned to the next minute boundary.
    #[arg(long)]
    pub start: String,
    /// Window end (exclusive). Same formats as `--start`.
    #[arg(long)]
    pub end: String,
    /// Optional JSON file overriding the canonical 32-feed registry.
    /// Shape: `[{"symbol":"SPY","session":"regular","feed_id":"…"}, …]`.
    #[arg(long)]
    pub feeds: Option<PathBuf>,
    /// Pyth Benchmarks endpoint base (no trailing slash).
    #[arg(long, default_value = DEFAULT_BENCHMARKS_URL_BASE)]
    pub benchmarks_url: String,
    /// `_source` stamped on every emitted row. The default
    /// `pyth:hermes:benchmarks` distinguishes backfill rows from the
    /// `pyth:hermes:launchd` rows the forward poll emits, so consumers
    /// can scope queries via `_source`.
    #[arg(long, default_value = "pyth:hermes:benchmarks")]
    pub source: String,
    /// HTTP request timeout in seconds (per call).
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,
    /// Sleep between bucket calls. 100ms = 10 req/s; respect
    /// the public Benchmarks rate-limit (inherits Hermes's). Tune up
    /// for a faster run if the upstream allows.
    #[arg(long, default_value_t = 100)]
    pub rate_limit_ms: u64,
    /// Bulk-write to parquet every N buckets to keep memory bounded.
    /// 1440 = 1 day at 60s grain — typical run flushes once per
    /// calendar day.
    #[arg(long, default_value_t = 1440)]
    pub flush_every_buckets: u64,
    /// Verbose progress every N buckets. Default: every 60 buckets
    /// (~1 minute of wall-clock at 10 req/s) so the operator can tail
    /// the log.
    #[arg(long, default_value_t = 60)]
    pub progress_every_buckets: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    pub dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::PYTH)]
    pub venue: String,
}

#[derive(serde::Deserialize, Debug)]
struct FeedFileEntry {
    symbol: String,
    session: String,
    feed_id: String,
}

pub async fn run_backfill(args: BackfillArgs) -> Result<()> {
    // Parse window. End is exclusive.
    let start_unix = crate::parse_unix_seconds(&args.start).context("parsing --start")?;
    let end_unix = crate::parse_unix_seconds(&args.end).context("parsing --end")?;
    if end_unix <= start_unix {
        anyhow::bail!("--end ({end_unix}) must be > --start ({start_unix})");
    }

    let interval_secs: u32 = 60;
    // Align start UP to the next minute boundary to keep poll_ts
    // values clean (every :00 second of UTC).
    let start_aligned = align_up_to_minute(start_unix);
    let end_aligned = align_down_to_minute(end_unix);
    if end_aligned <= start_aligned {
        anyhow::bail!(
            "after minute-alignment, --start ({start_aligned}) >= --end ({end_aligned}); \
             window too narrow"
        );
    }
    let total_buckets =
        ((end_aligned - start_aligned) / interval_secs as i64) as u64;

    let feeds: Vec<FeedRef> = if let Some(path) = &args.feeds {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading feed registry {}", path.display()))?;
        let parsed: Vec<FeedFileEntry> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing feed registry {}", path.display()))?;
        parsed
            .into_iter()
            .map(|e| FeedRef {
                symbol: e.symbol,
                session: e.session,
                feed_id: e.feed_id,
            })
            .collect()
    } else {
        default_feed_refs()
    };
    if feeds.is_empty() {
        anyhow::bail!("feed registry is empty");
    }

    let cfg = BackfillConfig {
        benchmarks_url_base: args.benchmarks_url.clone(),
        interval_secs,
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        ..BackfillConfig::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;
    let ds = Dataset::new(&args.dataset);

    tracing::info!(
        feeds = feeds.len(),
        start_unix = start_aligned,
        end_unix = end_aligned,
        total_buckets,
        rate_limit_ms = args.rate_limit_ms,
        flush_every = args.flush_every_buckets,
        "starting Pyth Benchmarks backfill"
    );

    let mut rows_buf: Vec<pyth::v1::Reading> = Vec::with_capacity(
        (args.flush_every_buckets as usize) * feeds.len(),
    );
    let mut total_rows_added: u64 = 0;
    let mut total_rows_deduped: u64 = 0;
    let mut total_partitions: u64 = 0;
    let mut total_empty_buckets: u64 = 0;
    let mut total_failed_buckets: u64 = 0;

    let started = std::time::Instant::now();
    let mut bucket_idx: u64 = 0;
    let mut anchor_unix = start_aligned;

    while anchor_unix < end_aligned {
        // Window covers [anchor_unix, anchor_unix + interval_secs];
        // poll_ts label is the WINDOW-END (anchor_unix + interval).
        // That keeps `pyth_age_s = poll_unix - publish_time` >= 0
        // and matches forward-poll's "we sampled at T, the publish
        // was at T-Δ" semantics.
        let bucket_start = anchor_unix;
        let bucket_end = anchor_unix + interval_secs as i64;
        let poll_unix = bucket_end;
        let poll_ts = format_iso_second(poll_unix);
        let meta = Meta::new(pyth::v1::SCHEMA_VERSION, poll_unix, &args.source);

        match poll_window(
            &client,
            &cfg,
            &feeds,
            bucket_start,
            poll_unix,
            &poll_ts,
            &meta,
        )
        .await
        {
            Ok(rows) => {
                if rows.is_empty() {
                    total_empty_buckets += 1;
                } else {
                    rows_buf.extend(rows);
                }
            }
            Err(e) => {
                tracing::warn!(
                    bucket_start,
                    bucket_end,
                    error = %e,
                    "bucket poll failed; continuing"
                );
                total_failed_buckets += 1;
            }
        }

        bucket_idx += 1;

        // Flush at the configured cadence OR at the end of the window.
        let last_bucket = anchor_unix + interval_secs as i64 >= end_aligned;
        if (bucket_idx % args.flush_every_buckets == 0 || last_bucket) && !rows_buf.is_empty() {
            let stats = ds
                .write::<pyth::v1::Reading>(&args.venue, None, &rows_buf)
                .with_context(|| {
                    format!("Dataset::write at bucket_idx={bucket_idx}")
                })?;
            total_rows_added += stats.rows_added as u64;
            total_rows_deduped += stats.rows_deduped as u64;
            total_partitions += stats.partitions_written as u64;
            tracing::info!(
                flushed_rows = rows_buf.len(),
                rows_added = stats.rows_added,
                rows_deduped = stats.rows_deduped,
                partitions = stats.partitions_written,
                "bucket flush"
            );
            rows_buf.clear();
        }

        if bucket_idx % args.progress_every_buckets == 0 {
            let elapsed = started.elapsed().as_secs_f64();
            let pct = (bucket_idx as f64 / total_buckets as f64) * 100.0;
            let eta_s = if bucket_idx > 0 {
                (elapsed / bucket_idx as f64) * (total_buckets - bucket_idx) as f64
            } else {
                0.0
            };
            tracing::info!(
                bucket_idx,
                total_buckets,
                pct = format!("{pct:.1}%"),
                elapsed_s = format!("{elapsed:.0}"),
                eta_s = format!("{eta_s:.0}"),
                empty_buckets = total_empty_buckets,
                failed_buckets = total_failed_buckets,
                "progress"
            );
        }

        if args.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.rate_limit_ms)).await;
        }

        anchor_unix += interval_secs as i64;
    }

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "pyth_backfill complete: buckets={} elapsed_s={:.0} rows_added={} rows_deduped={} \
         partitions={} empty_buckets={} failed_buckets={}",
        total_buckets,
        elapsed,
        total_rows_added,
        total_rows_deduped,
        total_partitions,
        total_empty_buckets,
        total_failed_buckets
    );
    Ok(())
}

fn align_up_to_minute(unix: i64) -> i64 {
    let r = unix.rem_euclid(60);
    if r == 0 {
        unix
    } else {
        unix + (60 - r)
    }
}

fn align_down_to_minute(unix: i64) -> i64 {
    unix - unix.rem_euclid(60)
}

/// Match the live tape's `poll_ts` shape exactly: ISO 8601 second-
/// precision UTC with `+00:00` suffix (NOT `Z`). See `pyth_cmd::run_tape`
/// for the same conversion in the forward path.
fn format_iso_second(unix: i64) -> String {
    let dt: DateTime<Utc> =
        Utc.timestamp_opt(unix, 0).single().unwrap_or_else(|| {
            // Should never happen for sane unix seconds; fall back to
            // a deterministic placeholder rather than panic.
            Utc.timestamp_opt(0, 0).unwrap()
        });
    let s = dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    s.replace('Z', "+00:00")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_idempotent_on_minute_boundary() {
        assert_eq!(align_up_to_minute(1_777_474_800), 1_777_474_800);
        assert_eq!(align_up_to_minute(1_777_474_801), 1_777_474_860);
        assert_eq!(align_up_to_minute(1_777_474_859), 1_777_474_860);
    }

    #[test]
    fn align_down_idempotent_on_minute_boundary() {
        assert_eq!(align_down_to_minute(1_777_474_800), 1_777_474_800);
        assert_eq!(align_down_to_minute(1_777_474_859), 1_777_474_800);
        assert_eq!(align_down_to_minute(1_777_474_801), 1_777_474_800);
    }

    #[test]
    fn iso_second_matches_pyth_cmd_format() {
        // 2026-04-29T16:00:00 UTC
        let s = format_iso_second(1_777_478_400);
        assert_eq!(s, "2026-04-29T16:00:00+00:00");
    }

    #[test]
    fn align_handles_negative_unix_safely() {
        // Sanity: rem_euclid handles negatives (rem returns 0..60).
        assert_eq!(align_up_to_minute(-1), 0);
        assert_eq!(align_down_to_minute(-1), -60);
    }

    fn p(date_str: &str) -> chrono::NaiveDate {
        chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn window_size_one_day_yields_1440_buckets() {
        let start = Utc
            .from_utc_datetime(&p("2026-04-29").and_hms_opt(0, 0, 0).unwrap())
            .timestamp();
        let end = Utc
            .from_utc_datetime(&p("2026-04-30").and_hms_opt(0, 0, 0).unwrap())
            .timestamp();
        let total = ((end - start) / 60) as u64;
        assert_eq!(total, 1440);
    }
}
