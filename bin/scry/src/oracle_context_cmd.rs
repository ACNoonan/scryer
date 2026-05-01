//! `scry solana oracle-context` — cross-source oracle observations
//! around each liquidation event.
//!
//! Pure offline tape-join: reads liquidation events from
//! kamino_liquidation.v1 / jupiter_lend_liquidation.v1 parquet, then
//! joins against the four continuously-collected oracle/price tapes
//! (kamino_scope, pyth, v5_tape, redstone) to find the closest pre/
//! post readings within a configurable window. No RPC.
//!
//! Output schema: `oracle_context.v1::Observation`. One row per
//! `(event, source[, session])` triple where any pre/post is in
//! window. Daily, no-key partition under
//! `dataset/oracle_context/observations/v1/year=Y/month=M/day=D.parquet`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_schema::oracle_context::v1::Observation;
use scryer_schema::Meta;
use scryer_schema::{kamino_scope, oracle_context, pyth, redstone, v5_tape};
use scryer_store::{
    read_liquidation_events, venue, Dataset, LiquidationEvent, UtcDay,
};

const PYTH_SESSIONS: &[&str] = &["regular", "pre", "post", "on"];

#[derive(Parser, Debug)]
pub struct OracleContextArgs {
    /// Path to a parquet file or directory tree containing liquidation
    /// events (kamino_liquidation.v1 / jupiter_lend_liquidation.v1).
    /// Both schemas are sniffed via column presence — point this at
    /// `dataset/kamino/liquidations/v1` or
    /// `dataset/jupiter_lend/liquidations/v1` (or higher).
    #[arg(long)]
    signatures_from: PathBuf,

    /// Half-window for pre/post search in seconds. The closest tape
    /// reading <= event_block_time within `window_secs` becomes
    /// `pre`; the closest > event_block_time within the same window
    /// becomes `post`. Default 300s (±5 minutes).
    #[arg(long, default_value_t = 300)]
    window_secs: i64,

    /// Optional cap on input events processed (dry-run convenience).
    #[arg(long)]
    limit: Option<usize>,

    /// Stamped on every emitted row's `_source`.
    #[arg(long, default_value = "tape-join")]
    source: String,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,

    #[arg(long, default_value = venue::ORACLE_CONTEXT)]
    venue: String,
}

/// Sorted-by-ts sequence of `(unix_ts, price)` pairs for a single
/// (source, key) bucket. Searched via `partition_point`.
type Series = Vec<(i64, f64)>;

#[derive(Default)]
struct TapeIndex {
    /// keyed by `symbol`. Used by scope, redstone, v5(chainlink),
    /// v5(jupiter_mid).
    by_symbol: BTreeMap<String, Series>,
    /// keyed by `(symbol, session)`. Used by pyth.
    by_symbol_session: BTreeMap<(String, String), Series>,
}

pub async fn run_oracle_context(args: OracleContextArgs) -> Result<()> {
    let now = Utc::now();
    let fetched_at = now.timestamp();

    // 1. Load events.
    let mut events = read_liquidation_events(&args.signatures_from)
        .with_context(|| format!("reading liquidation events from {}", args.signatures_from.display()))?;
    let total_events = events.len();
    if let Some(n) = args.limit {
        events.truncate(n);
    }
    if events.is_empty() {
        println!("oracle_context: no input events; nothing to do");
        return Ok(());
    }

    // 2. Determine the day-range we need to load tapes over.
    let min_ts = events.iter().map(|e| e.block_time).min().unwrap() - args.window_secs;
    let max_ts = events.iter().map(|e| e.block_time).max().unwrap() + args.window_secs;
    let days: Vec<UtcDay> = day_range(min_ts, max_ts);

    tracing::info!(
        events_total = total_events,
        events_processing = events.len(),
        window_secs = args.window_secs,
        days_to_load = days.len(),
        "oracle_context tape-join starting"
    );

    let ds = Dataset::new(&args.dataset);

    // 3. Load each tape into in-memory indexes. Missing partitions
    //    are silently skipped (the tape may not have run on that day).
    let scope_idx = load_scope_index(&ds, &days)?;
    let pyth_idx = load_pyth_index(&ds, &days)?;
    let v5_idx_chainlink = load_v5_chainlink_index(&ds, &days)?;
    let v5_idx_jupiter = load_v5_jupiter_index(&ds, &days)?;
    let redstone_idx = load_redstone_index(&ds, &days)?;

    tracing::info!(
        scope_symbols = scope_idx.by_symbol.len(),
        pyth_keys = pyth_idx.by_symbol_session.len(),
        chainlink_symbols = v5_idx_chainlink.by_symbol.len(),
        jupiter_symbols = v5_idx_jupiter.by_symbol.len(),
        redstone_symbols = redstone_idx.by_symbol.len(),
        "tape indexes loaded"
    );

    // 4. Join.
    let meta = Meta::new(oracle_context::v1::SCHEMA_VERSION, fetched_at, &args.source);
    let mut rows: Vec<Observation> = Vec::new();
    let mut events_with_any_match: usize = 0;
    for event in &events {
        let mut event_matched = false;
        for symbol in &event.symbols {
            // scope
            if let Some(o) = make_obs_keyed(&scope_idx.by_symbol, symbol, "scope", None, event, args.window_secs, &meta) {
                rows.push(o);
                event_matched = true;
            }
            // pyth (per session)
            for s in PYTH_SESSIONS {
                let key = (symbol.clone(), (*s).to_string());
                if let Some(o) = make_obs_keyed_with_key(
                    &pyth_idx.by_symbol_session,
                    &key,
                    symbol,
                    "pyth",
                    Some((*s).to_string()),
                    event,
                    args.window_secs,
                    &meta,
                ) {
                    rows.push(o);
                    event_matched = true;
                }
            }
            // chainlink from v5_tape
            if let Some(o) = make_obs_keyed(&v5_idx_chainlink.by_symbol, symbol, "chainlink", None, event, args.window_secs, &meta) {
                rows.push(o);
                event_matched = true;
            }
            // jupiter_mid from v5_tape
            if let Some(o) = make_obs_keyed(&v5_idx_jupiter.by_symbol, symbol, "jupiter_mid", None, event, args.window_secs, &meta) {
                rows.push(o);
                event_matched = true;
            }
            // redstone
            if let Some(o) = make_obs_keyed(&redstone_idx.by_symbol, symbol, "redstone", None, event, args.window_secs, &meta) {
                rows.push(o);
                event_matched = true;
            }
        }
        if event_matched {
            events_with_any_match += 1;
        }
    }

    if rows.is_empty() {
        println!(
            "oracle_context: events_processed={} events_matched=0 rows=0 (no tape coverage in window)",
            events.len()
        );
        return Ok(());
    }

    // 5. Write.
    let stats = ds
        .write::<Observation>(&args.venue, None, &rows)
        .context("Dataset::write oracle_context")?;
    println!(
        "oracle_context: events_processed={} events_matched={} rows_added={} rows_deduped={} partitions_written={}",
        events.len(),
        events_with_any_match,
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written
    );
    Ok(())
}

fn day_range(min_ts: i64, max_ts: i64) -> Vec<UtcDay> {
    if max_ts < min_ts {
        return Vec::new();
    }
    let Some(first) = UtcDay::from_unix_seconds(min_ts) else {
        return Vec::new();
    };
    let Some(last) = UtcDay::from_unix_seconds(max_ts) else {
        return vec![first];
    };
    let mut out = vec![first];
    if first == last {
        return out;
    }
    // Iterate by 86400-sec steps starting from min_ts and emit a new
    // day whenever crossing a UTC midnight. Stop once `last` is hit.
    let mut ts = min_ts;
    while out.len() < 400 {
        ts += 86_400;
        let Some(d) = UtcDay::from_unix_seconds(ts) else {
            break;
        };
        if d != *out.last().unwrap() {
            out.push(d);
        }
        if d == last {
            break;
        }
        if ts > max_ts + 86_400 {
            break;
        }
    }
    out
}

fn load_scope_index(ds: &Dataset, days: &[UtcDay]) -> Result<TapeIndex> {
    let mut idx = TapeIndex::default();
    for day in days {
        let rows: Vec<kamino_scope::v1::Reading> = ds
            .read::<kamino_scope::v1::Reading>(venue::KAMINO_SCOPE, None, *day)
            .context("read kamino_scope tape")?;
        for r in rows {
            if r.scope_err.is_some() {
                continue;
            }
            idx.by_symbol
                .entry(r.symbol.clone())
                .or_default()
                .push((r.scope_unix_ts, r.scope_price));
        }
    }
    sort_indexes(&mut idx);
    Ok(idx)
}

fn load_pyth_index(ds: &Dataset, days: &[UtcDay]) -> Result<TapeIndex> {
    let mut idx = TapeIndex::default();
    for day in days {
        let rows: Vec<pyth::v1::Reading> = ds
            .read::<pyth::v1::Reading>(venue::PYTH, None, *day)
            .context("read pyth tape")?;
        for r in rows {
            if r.pyth_err.is_some() {
                continue;
            }
            // pyth_publish_time is the upstream-asserted observation
            // time; falls back to poll_unix if missing/zero.
            let ts = if r.pyth_publish_time > 0 {
                r.pyth_publish_time
            } else {
                r.poll_unix
            };
            idx.by_symbol_session
                .entry((r.symbol.clone(), r.session.clone()))
                .or_default()
                .push((ts, r.pyth_price));
        }
    }
    sort_indexes(&mut idx);
    Ok(idx)
}

fn load_v5_chainlink_index(ds: &Dataset, days: &[UtcDay]) -> Result<TapeIndex> {
    let mut idx = TapeIndex::default();
    for day in days {
        let rows: Vec<v5_tape::v1::Reading> = ds
            .read::<v5_tape::v1::Reading>(venue::SOOTHSAYER_V5, None, *day)
            .context("read v5_tape (chainlink)")?;
        for r in rows {
            // Chainlink readings are nullable across the v5_tape row;
            // require both the obs ts and the tokenized px.
            let (Some(obs_ts), Some(px)) = (r.cl_obs_ts, r.cl_tokenized_px) else {
                continue;
            };
            idx.by_symbol
                .entry(r.symbol.clone())
                .or_default()
                .push((obs_ts, px));
        }
    }
    sort_indexes(&mut idx);
    Ok(idx)
}

fn load_v5_jupiter_index(ds: &Dataset, days: &[UtcDay]) -> Result<TapeIndex> {
    let mut idx = TapeIndex::default();
    for day in days {
        let rows: Vec<v5_tape::v1::Reading> = ds
            .read::<v5_tape::v1::Reading>(venue::SOOTHSAYER_V5, None, *day)
            .context("read v5_tape (jupiter_mid)")?;
        for r in rows {
            // Jupiter mid is required (real-time DEX-side; never null
            // when the tape ran successfully). Use poll_ts as the
            // observation timestamp.
            if r.jup_mid <= 0.0 || !r.jup_err.is_empty() {
                continue;
            }
            idx.by_symbol
                .entry(r.symbol.clone())
                .or_default()
                .push((r.poll_ts, r.jup_mid));
        }
    }
    sort_indexes(&mut idx);
    Ok(idx)
}

fn load_redstone_index(ds: &Dataset, days: &[UtcDay]) -> Result<TapeIndex> {
    let mut idx = TapeIndex::default();
    for day in days {
        let rows: Vec<redstone::v1::Reading> = ds
            .read::<redstone::v1::Reading>(venue::REDSTONE, None, *day)
            .context("read redstone tape")?;
        for r in rows {
            // redstone_ts is microseconds; the observation timestamp
            // is what we partition on at write time and what we want
            // to compare against event_block_time (seconds).
            let ts_secs = r.redstone_ts / 1_000_000;
            idx.by_symbol
                .entry(r.symbol.clone())
                .or_default()
                .push((ts_secs, r.value));
        }
    }
    sort_indexes(&mut idx);
    Ok(idx)
}

fn sort_indexes(idx: &mut TapeIndex) {
    for v in idx.by_symbol.values_mut() {
        v.sort_by_key(|(ts, _)| *ts);
    }
    for v in idx.by_symbol_session.values_mut() {
        v.sort_by_key(|(ts, _)| *ts);
    }
}

/// Find pre/post entries in `series` bracketing `target_ts` within
/// `±window_secs`. Series must be sorted ascending by ts.
fn find_pre_post(
    series: &Series,
    target_ts: i64,
    window_secs: i64,
) -> (Option<(i64, f64)>, Option<(i64, f64)>) {
    if series.is_empty() {
        return (None, None);
    }
    // partition_point returns first index whose ts > target_ts.
    let idx = series.partition_point(|(ts, _)| *ts <= target_ts);
    let pre = if idx > 0 {
        let (ts, px) = series[idx - 1];
        if target_ts - ts <= window_secs {
            Some((ts, px))
        } else {
            None
        }
    } else {
        None
    };
    let post = if idx < series.len() {
        let (ts, px) = series[idx];
        if ts - target_ts <= window_secs {
            Some((ts, px))
        } else {
            None
        }
    } else {
        None
    };
    (pre, post)
}

fn make_obs_keyed(
    map: &BTreeMap<String, Series>,
    symbol: &str,
    source: &str,
    session: Option<String>,
    event: &LiquidationEvent,
    window_secs: i64,
    meta: &Meta,
) -> Option<Observation> {
    let series = map.get(symbol)?;
    build_obs(series, symbol, source, session, event, window_secs, meta)
}

fn make_obs_keyed_with_key(
    map: &BTreeMap<(String, String), Series>,
    key: &(String, String),
    symbol: &str,
    source: &str,
    session: Option<String>,
    event: &LiquidationEvent,
    window_secs: i64,
    meta: &Meta,
) -> Option<Observation> {
    let series = map.get(key)?;
    build_obs(series, symbol, source, session, event, window_secs, meta)
}

fn build_obs(
    series: &Series,
    symbol: &str,
    source: &str,
    session: Option<String>,
    event: &LiquidationEvent,
    window_secs: i64,
    meta: &Meta,
) -> Option<Observation> {
    let (pre, post) = find_pre_post(series, event.block_time, window_secs);
    if pre.is_none() && post.is_none() {
        return None;
    }
    Some(Observation {
        signature: event.signature.clone(),
        symbol: symbol.to_string(),
        event_slot: event.slot,
        event_block_time: event.block_time,
        source: source.to_string(),
        session,
        pre_price: pre.map(|(_, p)| p),
        pre_unix_ts: pre.map(|(t, _)| t),
        pre_age_secs: pre.map(|(t, _)| event.block_time - t),
        post_price: post.map(|(_, p)| p),
        post_unix_ts: post.map(|(t, _)| t),
        post_age_secs: post.map(|(t, _)| t - event.block_time),
        meta: meta.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(oracle_context::v1::SCHEMA_VERSION, 1_777_300_000, "tape-join")
    }

    fn ev(sig: &str, block_time: i64) -> LiquidationEvent {
        LiquidationEvent {
            signature: sig.to_string(),
            slot: 415_581_004,
            block_time,
            symbols: vec!["SPYx".to_string()],
        }
    }

    #[test]
    fn find_pre_post_picks_closest_within_window() {
        // ts series with target_ts = 100; window = 30
        // Should pick ts=80 as pre (in window), ts=110 as post (in window).
        let series: Series = vec![(50, 700.0), (80, 710.0), (110, 715.0), (180, 720.0)];
        let (pre, post) = find_pre_post(&series, 100, 30);
        assert_eq!(pre, Some((80, 710.0)));
        assert_eq!(post, Some((110, 715.0)));
    }

    #[test]
    fn find_pre_post_drops_out_of_window_neighbors() {
        // Both nearest entries are outside the window.
        let series: Series = vec![(10, 700.0), (200, 720.0)];
        let (pre, post) = find_pre_post(&series, 100, 30);
        assert_eq!(pre, None);
        assert_eq!(post, None);
    }

    #[test]
    fn find_pre_post_handles_empty() {
        let s: Series = Vec::new();
        let (pre, post) = find_pre_post(&s, 100, 30);
        assert_eq!(pre, None);
        assert_eq!(post, None);
    }

    #[test]
    fn find_pre_post_at_target_ts_falls_into_pre() {
        // partition_point uses ts <= target_ts → idx points past
        // target, so target itself is treated as pre.
        let series: Series = vec![(100, 712.0), (130, 714.0)];
        let (pre, post) = find_pre_post(&series, 100, 30);
        assert_eq!(pre, Some((100, 712.0)));
        assert_eq!(post, Some((130, 714.0)));
    }

    #[test]
    fn build_obs_skips_when_both_sides_empty() {
        let series: Series = vec![(0, 700.0), (200, 720.0)];
        let m = meta();
        let result = build_obs(&series, "SPYx", "scope", None, &ev("sig-x", 100), 30, &m);
        assert!(result.is_none());
    }

    #[test]
    fn build_obs_emits_one_sided_observation() {
        // Only post is in window.
        let series: Series = vec![(50, 700.0), (110, 715.0)];
        let m = meta();
        let obs = build_obs(&series, "SPYx", "scope", None, &ev("sig-x", 100), 20, &m).unwrap();
        assert_eq!(obs.pre_price, None);
        assert_eq!(obs.post_price, Some(715.0));
        assert_eq!(obs.post_age_secs, Some(10));
    }

    #[test]
    fn build_obs_computes_age_secs() {
        let series: Series = vec![(80, 710.0), (110, 715.0)];
        let m = meta();
        let obs = build_obs(&series, "SPYx", "scope", None, &ev("sig-x", 100), 30, &m).unwrap();
        assert_eq!(obs.pre_age_secs, Some(20));
        assert_eq!(obs.post_age_secs, Some(10));
    }

    #[test]
    fn day_range_computes_inclusive_span() {
        // 2026-04-26 03:20 UTC to 2026-04-28 14:26 UTC → 3 days
        let days = day_range(1_777_260_000, 1_777_473_000);
        assert_eq!(days.len(), 3);
    }

    #[test]
    fn day_range_same_day_returns_one() {
        // 2026-04-26 03:20 UTC to 2026-04-26 14:26 UTC → 1 day
        let days = day_range(1_777_260_000, 1_777_300_000);
        assert_eq!(days.len(), 1);
    }

    #[test]
    fn day_range_crosses_midnight() {
        // 2026-04-25 23:00 UTC to 2026-04-26 00:53 UTC → 2 days
        let days = day_range(1_777_244_400, 1_777_251_200);
        assert_eq!(days.len(), 2);
    }
}
