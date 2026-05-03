//! `scry nasdaq halts-intraday` — fetch 1-minute Yahoo intraday bars
//! around each `nasdaq_halts.v1` event in the lookback window. Closes
//! Soothsayer W6's oracle-band-coverage analysis (Paper-3 §Structural
//! complement).
//!
//! The command reads halts from `dataset/nasdaq/halts/v1/` (yearly,
//! no key), filters to the requested lookback window, fetches Yahoo
//! `/v8/finance/chart` 1m bars per (symbol, halt_date) pair, and
//! writes one `nasdaq_halts_intraday.v1::Bar` row per (halt_event_id,
//! ts) tuple. The same minute may be written under multiple
//! halt_event_ids if a symbol gets halted multiple times the same
//! day — that's intentional so per-event joins stay simple.
//!
//! # Window choice
//!
//! For each halt event we fetch a 2-day Yahoo window starting at
//! `halt_date 00:00 UTC` (covers same-day halts AND late-day halts
//! that resume next morning). Yahoo returns regular-session bars
//! only because we set `includePrePost=false` upstream; rows outside
//! the symbol's listed market session simply don't exist.
//!
//! # Yahoo's 7-day backfill horizon
//!
//! `interval=1m` chart requests with `period1` older than 7 days
//! return an empty `timestamp[]`. Halts older than that simply
//! cannot be captured from this source — Soothsayer-side analyses
//! must disclose the missing halts or promote a paid intraday venue.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Utc};
use clap::Parser;
use scryer_fetch_equities::yahoo_intraday::{self, FetchError as YahooError, RawBar};
use scryer_schema::{nasdaq_halts, nasdaq_halts_intraday, Meta};
use scryer_store::{venue, Dataset, PartitionTime};

#[derive(Parser, Debug)]
pub struct HaltsIntradayArgs {
    /// How many days of halt history to scan, counting backward from
    /// today (UTC). Capped operationally at 7 because Yahoo's 1m
    /// chart endpoint only serves a 7-day rolling window — older
    /// halts cannot be backfilled from this source.
    #[arg(long, default_value_t = 7)]
    lookback_days: i64,
    /// Optional restriction: comma-separated tickers to filter the
    /// halt set down to (matches `nasdaq_halts.v1::Halt::underlying`).
    /// Default: every halted symbol in the lookback window. The
    /// Soothsayer universe (SPY, QQQ, …) very rarely halts; the
    /// "all halts" default ensures the dataset is non-empty for
    /// downstream W6 analysis.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// `_source` stamped on every emitted bar.
    #[arg(long, default_value = yahoo_intraday::SOURCE_LABEL)]
    source: String,
    /// Yahoo chart endpoint base URL.
    #[arg(long, default_value = yahoo_intraday::DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-call spacing in milliseconds. Yahoo's chart endpoint
    /// rate-limit kicks in around ~1-2 req/sec from a single IP;
    /// 600ms keeps a small safety margin.
    #[arg(long, default_value_t = 600)]
    rate_limit_ms: u64,
    /// `nasdaq_halts.v1` venue. Override only if the operator
    /// staged halts under a non-default venue path.
    #[arg(long, default_value = venue::NASDAQ)]
    halts_venue: String,
    /// Output venue for `nasdaq_halts_intraday.v1::Bar`.
    #[arg(long, default_value = venue::NASDAQ)]
    venue: String,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
}

pub async fn run_halts_intraday(args: HaltsIntradayArgs) -> Result<()> {
    if args.lookback_days <= 0 || args.lookback_days > 7 {
        anyhow::bail!(
            "--lookback-days must be in 1..=7 (Yahoo 1m horizon caps at 7 days); got {}",
            args.lookback_days
        );
    }
    let cfg = yahoo_intraday::PollConfig {
        base_url: args.base_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        ..Default::default()
    };
    let client = yahoo_intraday::build_client(&cfg).context("building reqwest client")?;
    let ds = Dataset::new(&args.dataset);
    let now = Utc::now();
    let cutoff_unix = now.timestamp() - args.lookback_days * 86_400;
    let cutoff_date32 = (cutoff_unix / 86_400) as i32;

    // Load the current year's halt partition; if the lookback window
    // crosses a Jan-1 boundary, also pull the prior year's. Yearly
    // partitions are tiny (~10⁴ rows/year), so reading the whole
    // year twice is cheap.
    let mut years_to_load: BTreeSet<i32> = BTreeSet::new();
    years_to_load.insert(now.year());
    let lookback_dt = Utc
        .timestamp_opt(cutoff_unix, 0)
        .single()
        .context("cutoff timestamp out of range")?;
    if lookback_dt.year() != now.year() {
        years_to_load.insert(lookback_dt.year());
    }

    let symbol_filter: BTreeSet<String> = args.symbols.iter().cloned().collect();

    let mut halts_in_window: Vec<nasdaq_halts::v1::Halt> = Vec::new();
    for year in &years_to_load {
        let rows: Vec<nasdaq_halts::v1::Halt> = ds
            .read::<nasdaq_halts::v1::Halt>(
                &args.halts_venue,
                None,
                PartitionTime::Yearly(*year),
            )
            .with_context(|| format!("read nasdaq_halts/v1/year={year}"))?;
        for h in rows {
            if h.halt_date < cutoff_date32 {
                continue;
            }
            if !symbol_filter.is_empty() && !symbol_filter.contains(&h.underlying) {
                continue;
            }
            halts_in_window.push(h);
        }
    }

    // Collapse to one Yahoo fetch per (symbol, halt_date) — every
    // halt sharing a (symbol, halt_date) wants the same 2-day window
    // of bars, and the FK column lets us tag each bar with all the
    // halt_event_ids for that day. One Yahoo call per (symbol,
    // halt_date) instead of one per halt event.
    let mut by_day: BTreeMap<(String, i32), Vec<String>> = BTreeMap::new();
    for h in &halts_in_window {
        by_day
            .entry((h.underlying.clone(), h.halt_date))
            .or_default()
            .push(h.dedup_key());
    }

    tracing::info!(
        halt_events = halts_in_window.len(),
        unique_day_symbol_pairs = by_day.len(),
        cutoff_date32,
        "scanning nasdaq halts"
    );

    if by_day.is_empty() {
        println!("nasdaq halts-intraday: rows_added=0 partitions_written=0 halts_in_window=0 (no halts to fetch)");
        return Ok(());
    }

    let now_unix = now.timestamp();
    let mut by_symbol: BTreeMap<String, Vec<nasdaq_halts_intraday::v1::Bar>> = BTreeMap::new();
    let mut errors: Vec<(String, i32, String)> = Vec::new();
    let mut symbols_with_bars: usize = 0;
    let mut total_raw_bars: usize = 0;

    for ((symbol, halt_date), event_ids) in &by_day {
        let period1 = (*halt_date as i64) * 86_400;
        let period2 = period1 + 2 * 86_400;
        match yahoo_intraday::fetch_intraday_1m(&client, &cfg, symbol, period1, period2).await {
            Ok(raw) => {
                if raw.is_empty() {
                    tracing::info!(
                        symbol,
                        halt_date,
                        "yahoo returned no bars (out of 7d horizon, delisted, or no prints)"
                    );
                } else {
                    symbols_with_bars += 1;
                }
                total_raw_bars += raw.len();
                let bars = build_bars(symbol, event_ids, &raw, now_unix, &args.source);
                if !bars.is_empty() {
                    by_symbol.entry(symbol.clone()).or_default().extend(bars);
                }
            }
            Err(e) => {
                tracing::warn!(symbol, halt_date, error = %e, "yahoo intraday fetch failed; continuing");
                errors.push((symbol.clone(), *halt_date, format_yahoo_error(&e)));
            }
        }
        if args.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.rate_limit_ms)).await;
        }
    }

    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, bars) in &by_symbol {
        let stats = ds
            .write::<nasdaq_halts_intraday::v1::Bar>(&args.venue, Some(symbol), bars)
            .with_context(|| format!("Dataset::write nasdaq_halts_intraday for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "nasdaq halts-intraday: rows_added={} rows_deduped={} partitions_written={} \
         halts_in_window={} day_symbol_pairs={} symbols_with_bars={} raw_bars={} symbols_failed={}",
        total_added,
        total_deduped,
        total_partitions,
        halts_in_window.len(),
        by_day.len(),
        symbols_with_bars,
        total_raw_bars,
        errors.len()
    );
    Ok(())
}

/// Build the per-(halt_event_id, ts) bars for one (symbol, halt_date)
/// fetch. Each raw bar gets duplicated once per halt_event_id sharing
/// that day; `(halt_event_id, ts)` is unique per emitted row so
/// dedup is well-defined.
fn build_bars(
    symbol: &str,
    event_ids: &[String],
    raw: &[RawBar],
    fetched_at: i64,
    source_label: &str,
) -> Vec<nasdaq_halts_intraday::v1::Bar> {
    let mut out = Vec::with_capacity(raw.len() * event_ids.len());
    for event_id in event_ids {
        for r in raw {
            out.push(nasdaq_halts_intraday::v1::Bar {
                symbol: symbol.to_string(),
                halt_event_id: event_id.clone(),
                ts: r.ts,
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                volume: r.volume,
                meta: Meta::new(
                    nasdaq_halts_intraday::v1::SCHEMA_VERSION,
                    fetched_at,
                    source_label,
                ),
            });
        }
    }
    out
}

fn format_yahoo_error(e: &YahooError) -> String {
    e.to_string()
}
