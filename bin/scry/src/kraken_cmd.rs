//! `scry kraken trades` — Kraken public REST trades fetcher.
//!
//! Closes the originally-locked v0.1 slice 2. Iterates Kraken's
//! `Trades` endpoint via the nanosecond-cursor pagination scheme and
//! writes each page to `dataset/kraken/trades/v1/pair=<P>/year=Y/
//! month=M/day=D.parquet` through scryer-store.
//!
//! See `methodology_log.md` "Kraken-spot-trades fetcher v0.1 —
//! 2026-05-01 (locked)" for endpoint, pagination, rate-limit, and
//! pair-name canonicalization decisions.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_cex_kraken::{fetch_page, PollConfig, DEFAULT_TRADES_URL};
use scryer_schema::trade::v1::{Trade, SCHEMA_VERSION};
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};

use crate::parse_unix_seconds;

#[derive(Parser, Debug)]
pub struct TradesArgs {
    /// Kraken altname pair (e.g. `SOLUSD`, `BTCUSD`, `ETHUSD`). The
    /// fetcher passes this verbatim to Kraken's `Trades` endpoint and
    /// uses it as the `pair=<X>` partition-path segment.
    /// Pair-canonicalization rationale: `methodology_log.md`
    /// "Kraken-spot-trades fetcher v0.1".
    #[arg(long)]
    pub pair: String,

    /// Window start (inclusive). Accepts `YYYY-MM-DD`, RFC 3339, or
    /// raw unix seconds. Required unless `--lookback-secs` is set.
    #[arg(long)]
    pub start: Option<String>,

    /// Window end (exclusive). Required unless `--lookback-secs` is
    /// set.
    #[arg(long)]
    pub end: Option<String>,

    /// launchd-tail mode. Computes `[now - lookback_secs, now)` and
    /// ignores `--start`/`--end`. The hourly tail plist sets this to
    /// 86400 (24h) so each tick re-fetches the prior day's window;
    /// dedup_key collapses re-fetches cleanly.
    #[arg(long)]
    pub lookback_secs: Option<i64>,

    /// `_source` label stamped on every emitted row. Default
    /// `kraken:Trades`. Override for launchd-tail rows
    /// (`kraken:Trades:launchd`) vs one-shot backfill
    /// (`kraken:Trades:backfill:2025-10-27..2026-04-25`) so consumers
    /// can scope queries via `_source LIKE '...'`.
    #[arg(long, default_value = "kraken:Trades")]
    pub source: String,

    /// Endpoint URL override. Defaults to `api.kraken.com`.
    #[arg(long, default_value = DEFAULT_TRADES_URL)]
    pub trades_url: String,

    /// Sustained delay between successive page fetches (ms).
    /// Default 1000ms — the safe ceiling for Kraken's
    /// unauthenticated tier under multi-hour backfills.
    #[arg(long, default_value_t = 1000)]
    pub rate_limit_ms: u64,

    /// Number of retries on transient failures before propagating.
    #[arg(long, default_value_t = 5)]
    pub retry_max: u32,

    /// Initial backoff for the exponential-retry path (ms). Doubles
    /// per attempt: 1s / 2s / 4s / 8s / 16s at the default.
    #[arg(long, default_value_t = 1000)]
    pub retry_initial_backoff_ms: u64,

    /// Per-request HTTP timeout (s). Kraken's free-tier response time
    /// can spike to 10s+ during peak load; default 30s.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,

    /// Output dataset root.
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    pub dataset: PathBuf,

    /// Venue string. Almost never overridden; pinned for the
    /// methodology-locked layout.
    #[arg(long, default_value = venue::KRAKEN)]
    pub venue: String,

    /// Log a progress line every N pages. Default 50.
    #[arg(long, default_value_t = 50)]
    pub progress_every: u32,
}

pub async fn run_trades(args: TradesArgs) -> Result<()> {
    let (start_unix, end_unix) = if let Some(lb) = args.lookback_secs {
        if lb <= 0 {
            anyhow::bail!("--lookback-secs must be > 0");
        }
        let now = Utc::now().timestamp();
        (now - lb, now)
    } else {
        let s = args
            .start
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--start required (or pass --lookback-secs N)"))?;
        let e = args
            .end
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--end required (or pass --lookback-secs N)"))?;
        let s_u = parse_unix_seconds(s).with_context(|| format!("parsing --start `{s}`"))?;
        let e_u = parse_unix_seconds(e).with_context(|| format!("parsing --end `{e}`"))?;
        (s_u, e_u)
    };
    if end_unix <= start_unix {
        anyhow::bail!("--end ({end_unix}) must be strictly after --start ({start_unix})");
    }
    if args.pair.is_empty() {
        anyhow::bail!("--pair cannot be empty");
    }

    let cfg = PollConfig {
        trades_url: args.trades_url.clone(),
        rate_limit_ms: args.rate_limit_ms,
        retry_max: args.retry_max,
        retry_initial_backoff_ms: args.retry_initial_backoff_ms,
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            "scryer-fetch-cex-kraken/",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .context("building reqwest client")?;

    let fetched_at = Utc::now().timestamp();
    let source = args.source.clone();
    let meta_ctor = move || Meta::new(SCHEMA_VERSION, fetched_at, source.clone());

    // Cursor: nanoseconds since unix epoch. Pass `start_unix * 1e9 - 1`
    // so trades exactly at start_unix (boundary) are included — Kraken
    // returns rows with `ts > since/1e9`.
    let mut since_ns: i64 = start_unix.saturating_mul(1_000_000_000).saturating_sub(1);
    let end_ns_inclusive: i64 = end_unix.saturating_mul(1_000_000_000);

    let ds = Dataset::new(&args.dataset);
    let start_unix_f = start_unix as f64;
    let end_unix_f = end_unix as f64;

    let mut pages: u32 = 0;
    let mut total_rows_seen: usize = 0;
    let mut total_in_window: usize = 0;
    let mut total_added: usize = 0;
    let mut total_deduped: usize = 0;

    tracing::info!(
        pair = %args.pair,
        start = start_unix,
        end = end_unix,
        rate_limit_ms = cfg.rate_limit_ms,
        "kraken trades: starting window",
    );

    loop {
        let page = fetch_page(&client, &cfg, &args.pair, since_ns, &meta_ctor)
            .await
            .with_context(|| format!("fetch_page pair={} since_ns={since_ns}", args.pair))?;
        pages += 1;
        total_rows_seen += page.trades.len();

        if page.trades.is_empty() {
            tracing::info!(
                page = pages,
                last_ns = page.last_ns,
                "kraken trades: empty page (caught up to tail); terminating",
            );
            break;
        }

        // Window filter. The lower bound is implicit (since-cursor
        // semantics) but we still filter explicitly to handle the
        // since_ns - 1 boundary nudge. Upper bound terminates the
        // window cleanly.
        let in_window: Vec<Trade> = page
            .trades
            .iter()
            .filter(|t| t.ts >= start_unix_f && t.ts < end_unix_f)
            .cloned()
            .collect();
        total_in_window += in_window.len();

        if !in_window.is_empty() {
            let stats = ds
                .write::<Trade>(&args.venue, Some(&args.pair), &in_window)
                .with_context(|| {
                    format!("Dataset::write venue={} pair={}", args.venue, args.pair)
                })?;
            total_added += stats.rows_added;
            total_deduped += stats.rows_deduped;
        }

        if pages.is_multiple_of(args.progress_every) {
            tracing::info!(
                page = pages,
                last_ns = page.last_ns,
                rows_seen = total_rows_seen,
                rows_in_window = total_in_window,
                rows_added = total_added,
                rows_deduped = total_deduped,
                "kraken trades: progress",
            );
        }

        // Termination guards. (1) cursor must advance; if Kraken
        // returns the same `last` we passed in, it's a stuck-cursor
        // signal and we stop to avoid an infinite loop. (2) once the
        // cursor crosses the end of the window, we're done.
        if page.last_ns <= since_ns {
            tracing::warn!(
                last_ns = page.last_ns,
                since_ns,
                "kraken trades: cursor did not advance; terminating",
            );
            break;
        }
        since_ns = page.last_ns;
        if since_ns >= end_ns_inclusive {
            break;
        }

        if cfg.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cfg.rate_limit_ms)).await;
        }
    }

    println!(
        "kraken trades: pages={pages} rows_seen={total_rows_seen} \
         rows_in_window={total_in_window} rows_added={total_added} \
         rows_deduped={total_deduped} pair={} window=[{start_unix},{end_unix})",
        args.pair,
    );
    Ok(())
}
