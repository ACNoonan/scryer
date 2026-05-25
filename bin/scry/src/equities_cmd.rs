//! `scry equities bars` + `scry equities earnings` — equity market-data
//! fetchers. Stooq for OHLCV daily bars, Finnhub for earnings dates.
//!
//! Replaces soothsayer's `scripts/run_v1_scrape.py`. Forward-running
//! cadence is wrapped externally by launchd (typical: daily after
//! US market close).
//!
//! Bars: per-symbol OHLCV daily bars over an arbitrary window,
//! written to `dataset/yahoo/equities_daily/v1/symbol={X}/year=YYYY.parquet`
//! (Yearly + symbol-keyed, per the existing `yahoo.v1` schema; the
//! schema name + venue path are historical from soothsayer's yfinance
//! era).
//!
//! Earnings: per-symbol upcoming + recent earnings dates, written to
//! `dataset/yahoo/earnings/v1/symbol={X}/year=YYYY.parquet`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use clap::Parser;
use scryer_fetch_equities::{
    finnhub, stooq, yahoo_corp_actions, yahoo_earnings, PollConfig, FetchError as EqFetchError,
};
use scryer_schema::{earnings, yahoo, yahoo_corp_actions as schema_corp_actions, Meta};
use scryer_store::{venue, Dataset, PartitionTime};

#[derive(Parser, Debug)]
pub struct BarsArgs {
    /// Comma-separated tickers in soothsayer/yfinance convention; the
    /// fetcher maps to Stooq syntax (`SPY` → `spy.us`, `ES=F` → `es.f`,
    /// `^VIX` → `^vix`, `BTC-USD` → `btcusd`). For Paper-1 parity use
    /// `SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT,ES=F,NQ=F,GC=F,ZN=F,^VIX,BTC-USD`.
    /// (^GVZ and ^MOVE are not on Stooq — fetcher will surface them
    /// as upstream errors; consumers can ignore.)
    #[arg(long, value_delimiter = ',', required = true)]
    symbols: Vec<String>,
    /// Window start as `YYYY-MM-DD` UTC. When unset, defaults to
    /// `--lookback-days` before today (UTC).
    #[arg(long, default_value = "")]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC. When unset, defaults to
    /// today (UTC).
    #[arg(long, default_value = "")]
    end: String,
    /// Rolling-window lookback in days when `--start`/`--end` are
    /// omitted. Used by the daily forward-poll manifest so the
    /// fetch keeps pulling the most recent N days of bars without
    /// having to re-stamp dates per fire. Stooq returns the full
    /// window; the store dedups on `(symbol, ts)` so re-pulls are
    /// idempotent.
    #[arg(long, default_value_t = 7)]
    lookback_days: i64,
    /// Stooq API key. Free, captcha-acquired at
    /// `https://stooq.com/q/d/?s=spy.us&get_apikey`. Defaults to the
    /// `STOOQ_API_KEY` env var (loaded from `./.env` via dotenvy).
    #[arg(long, env = "STOOQ_API_KEY")]
    apikey: String,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "stooq:csv")]
    source: String,
    /// Stooq base URL.
    #[arg(long, default_value = stooq::DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Delay between successive symbol calls in milliseconds. Stooq
    /// has a daily-hits limit on free tier; the default is gentle.
    #[arg(long, default_value_t = 500)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::YAHOO)]
    venue: String,
}

#[derive(Parser, Debug)]
pub struct EarningsArgs {
    /// Comma-separated tickers — typically only the equity underliers
    /// (SPY/QQQ are ETFs and have no earnings; futures and crypto
    /// also have none). Finnhub returns empty for non-equity symbols
    /// without erroring.
    #[arg(long, value_delimiter = ',', required = true)]
    symbols: Vec<String>,
    /// Window start as `YYYY-MM-DD` UTC. Defaults to 30 days ago.
    #[arg(long, default_value = "")]
    from: String,
    /// Window end as `YYYY-MM-DD` UTC. Defaults to 90 days ahead
    /// (free tier covers ~1y forward).
    #[arg(long, default_value = "")]
    to: String,
    /// Finnhub API key. Defaults to the `FINNHUB_API_KEY` env var
    /// (loaded from `./.env` via dotenvy).
    #[arg(long, env = "FINNHUB_API_KEY")]
    token: String,
    #[arg(long, default_value = "finnhub:earnings")]
    source: String,
    #[arg(long, default_value = finnhub::DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Delay between successive symbol calls. Finnhub free tier is
    /// 60 calls/min — default 1100ms keeps us safely under.
    #[arg(long, default_value_t = 1100)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::YAHOO)]
    venue: String,
}

#[derive(Parser, Debug)]
pub struct CorpActionsArgs {
    /// Comma-separated tickers — equity / ETF symbols whose
    /// dividend + split history is to be backfilled. Use the same
    /// case Yahoo expects (`AAPL`, `SPY`, `MSTR`).
    #[arg(long, value_delimiter = ',', required = true)]
    symbols: Vec<String>,
    /// Window start as `YYYY-MM-DD` UTC. Yahoo serves the entire
    /// history per symbol on every call; the fetcher clips response-
    /// side to `[start, end]`.
    #[arg(long)]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC.
    #[arg(long)]
    end: String,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "yahoo:chart")]
    source: String,
    /// Yahoo chart base URL.
    #[arg(long, default_value = yahoo_corp_actions::DEFAULT_BASE_URL)]
    base_url: String,
    /// Browser-shaped User-Agent. Yahoo's gate is sensitive to
    /// non-browser TLS clients; the default below matches what
    /// `yfinance` uses internally as of v0.2.43.
    #[arg(
        long,
        default_value = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )]
    user_agent: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Delay between successive symbol calls in milliseconds. Yahoo's
    /// per-IP rate-limit triggers around ~1-2 req/sec; default 1100ms
    /// keeps a small safety margin.
    #[arg(long, default_value_t = 1100)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::YAHOO)]
    venue: String,
}

/// `scry equities earnings-backfill` — one-shot deep-history earnings
/// pull from Yahoo's visualization API (the only free first-party
/// source with both deep history and an explicit session field).
/// Finnhub's free tier returns no history, so this is how `earnings.v2`
/// gets historical `bmo`/`amc` timing. Writes to `yahoo/earnings/v2/`.
#[derive(Parser, Debug)]
pub struct EarningsBackfillArgs {
    /// Comma-separated reporting-equity tickers (ETFs/futures/crypto
    /// have no earnings; Yahoo returns empty for them).
    #[arg(long, value_delimiter = ',', required = true)]
    symbols: Vec<String>,
    /// Max events returned per symbol. Yahoo caps at 250; 100 covers
    /// ~25 years of quarterly reports.
    #[arg(long, default_value_t = 100)]
    size: u32,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "yahoo:earnings:visualization")]
    source: String,
    /// Yahoo `query{1,2}` host the crumb is bound to.
    #[arg(long, default_value = yahoo_earnings::DEFAULT_BASE_URL)]
    base_url: String,
    /// Browser-shaped User-Agent — Yahoo's gate rejects non-browser
    /// TLS clients. Matches the corp-actions fetcher default.
    #[arg(
        long,
        default_value = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )]
    user_agent: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Delay between successive symbol calls. Yahoo's per-IP throttle
    /// trips around 1-2 req/sec; 1500ms keeps a margin for a one-shot.
    #[arg(long, default_value_t = 1500)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::YAHOO)]
    venue: String,
}

/// `scry equities earnings-migrate` — one-time cutover that re-expresses
/// existing `earnings.v1` rows as `earnings.v2` with `session=unknown`,
/// so the v2 path carries the full pre-existing date coverage even
/// where no source can supply timing.
///
/// MUST be run AFTER `earnings-backfill` (Yahoo) and the Finnhub
/// `earnings` runner: the store's dedup keeps the *existing* row on a
/// `(symbol, date)` collision, so any date already written with real
/// timing is preserved and only genuinely-uncovered dates receive an
/// `unknown` row. `rows_added` = gaps filled; `rows_deduped` = dates
/// that already had timing. v1 partitions are left untouched.
#[derive(Parser, Debug)]
pub struct EarningsMigrateArgs {
    /// Comma-separated tickers whose v1 partitions to migrate into v2.
    #[arg(long, value_delimiter = ',', required = true)]
    symbols: Vec<String>,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::YAHOO)]
    venue: String,
}

pub async fn run_bars(args: BarsArgs) -> Result<()> {
    if args.apikey.is_empty() {
        anyhow::bail!(
            "Stooq apikey required; pass --apikey or set STOOQ_API_KEY env var. Acquire one at https://stooq.com/q/d/?s=spy.us&get_apikey"
        );
    }
    if args.lookback_days <= 0 {
        anyhow::bail!("--lookback-days must be positive; got {}", args.lookback_days);
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
    let meta = Meta::new(yahoo::v1::SCHEMA_VERSION, now.timestamp(), &args.source);

    let end = if args.end.is_empty() {
        now.format("%Y-%m-%d").to_string()
    } else {
        args.end.clone()
    };
    let start = if args.start.is_empty() {
        (now - chrono::Duration::days(args.lookback_days))
            .format("%Y-%m-%d")
            .to_string()
    } else {
        args.start.clone()
    };

    tracing::info!(
        symbols = args.symbols.len(),
        start = %start,
        end = %end,
        "fetching Stooq bars"
    );

    let mut by_symbol: BTreeMap<String, Vec<yahoo::v1::Bar>> = BTreeMap::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    for symbol in &args.symbols {
        match stooq::fetch_bars(
            &client,
            &cfg,
            &args.base_url,
            &args.apikey,
            symbol,
            &start,
            &end,
            &meta,
        )
        .await
        {
            Ok(bars) => {
                tracing::info!(symbol, rows = bars.len(), "decoded");
                by_symbol.entry(symbol.clone()).or_default().extend(bars);
            }
            Err(e) => {
                tracing::warn!(symbol, error = %e, "fetch failed; continuing");
                errors.push((symbol.clone(), format_eq_error(&e)));
            }
        }
        if args.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.rate_limit_ms)).await;
        }
    }

    if by_symbol.values().all(|v| v.is_empty()) {
        if !errors.is_empty() {
            anyhow::bail!(
                "all {} symbol(s) failed; first error: {}",
                errors.len(),
                errors.first().map(|(_, e)| e.as_str()).unwrap_or("?")
            );
        }
        println!("equities bars: rows_added=0 rows_deduped=0 partitions_written=0 (empty)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, bars) in &by_symbol {
        if bars.is_empty() {
            continue;
        }
        let stats = ds
            .write::<yahoo::v1::Bar>(&args.venue, Some(symbol), bars)
            .with_context(|| format!("Dataset::write yahoo bars for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "equities bars: rows_added={} rows_deduped={} partitions_written={} symbols_failed={}",
        total_added,
        total_deduped,
        total_partitions,
        errors.len()
    );
    Ok(())
}

pub async fn run_earnings(args: EarningsArgs) -> Result<()> {
    if args.token.is_empty() {
        anyhow::bail!("Finnhub api key required; pass --token or set FINNHUB_API_KEY env var");
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
    let from = if args.from.is_empty() {
        (now - chrono::Duration::days(30)).format("%Y-%m-%d").to_string()
    } else {
        args.from.clone()
    };
    let to = if args.to.is_empty() {
        (now + chrono::Duration::days(90)).format("%Y-%m-%d").to_string()
    } else {
        args.to.clone()
    };
    let meta = Meta::new(earnings::v2::SCHEMA_VERSION, now.timestamp(), &args.source);

    tracing::info!(
        symbols = args.symbols.len(),
        from = from,
        to = to,
        "fetching Finnhub earnings"
    );

    let mut by_symbol: BTreeMap<String, Vec<earnings::v2::Event>> = BTreeMap::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    let mut symbols_with_earnings: usize = 0;
    for symbol in &args.symbols {
        match finnhub::fetch_earnings(
            &client,
            &cfg,
            &args.base_url,
            &args.token,
            symbol,
            &from,
            &to,
            &meta,
        )
        .await
        {
            Ok(events) => {
                if !events.is_empty() {
                    symbols_with_earnings += 1;
                }
                tracing::info!(symbol, rows = events.len(), "decoded");
                by_symbol.entry(symbol.clone()).or_default().extend(events);
            }
            Err(e) => {
                tracing::warn!(symbol, error = %e, "fetch failed; continuing");
                errors.push((symbol.clone(), format_eq_error(&e)));
            }
        }
        if args.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.rate_limit_ms)).await;
        }
    }

    if by_symbol.values().all(|v| v.is_empty()) {
        println!(
            "equities earnings: rows_added=0 partitions_written=0 symbols_with_earnings=0 symbols_failed={}",
            errors.len()
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, events) in &by_symbol {
        if events.is_empty() {
            continue;
        }
        let stats = ds
            .write::<earnings::v2::Event>(&args.venue, Some(symbol), events)
            .with_context(|| format!("Dataset::write yahoo earnings for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "equities earnings: rows_added={} rows_deduped={} partitions_written={} symbols_with_earnings={} symbols_failed={}",
        total_added,
        total_deduped,
        total_partitions,
        symbols_with_earnings,
        errors.len()
    );
    Ok(())
}

pub async fn run_earnings_backfill(args: EarningsBackfillArgs) -> Result<()> {
    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        source_label: args.source.clone(),
        user_agent: args.user_agent.clone(),
        ..Default::default()
    };

    let now = Utc::now();
    let meta = Meta::new(earnings::v2::SCHEMA_VERSION, now.timestamp(), &args.source);

    // One cookie+crumb handshake for the whole run; the crumb rides on
    // every visualization POST.
    let session = yahoo_earnings::bootstrap_session(&cfg, &args.base_url)
        .await
        .context("yahoo cookie+crumb bootstrap (one-shot backfill)")?;

    tracing::info!(symbols = args.symbols.len(), size = args.size, "fetching Yahoo earnings history");

    let mut by_symbol: BTreeMap<String, Vec<earnings::v2::Event>> = BTreeMap::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    let mut symbols_with_earnings = 0usize;
    for symbol in &args.symbols {
        match yahoo_earnings::fetch_earnings(
            &session,
            &cfg,
            &args.base_url,
            symbol,
            args.size,
            &meta,
        )
        .await
        {
            Ok(events) => {
                if !events.is_empty() {
                    symbols_with_earnings += 1;
                }
                tracing::info!(symbol, rows = events.len(), "decoded");
                by_symbol.entry(symbol.clone()).or_default().extend(events);
            }
            Err(e) => {
                tracing::warn!(symbol, error = %e, "fetch failed; continuing");
                errors.push((symbol.clone(), format_eq_error(&e)));
            }
        }
        if args.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.rate_limit_ms)).await;
        }
    }

    if by_symbol.values().all(|v| v.is_empty()) {
        if !errors.is_empty() {
            anyhow::bail!(
                "all {} symbol(s) failed; first error: {}",
                errors.len(),
                errors.first().map(|(_, e)| e.as_str()).unwrap_or("?")
            );
        }
        println!("equities earnings-backfill: rows_added=0 partitions_written=0 (empty)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, events) in &by_symbol {
        if events.is_empty() {
            continue;
        }
        let stats = ds
            .write::<earnings::v2::Event>(&args.venue, Some(symbol), events)
            .with_context(|| format!("Dataset::write yahoo earnings (backfill) for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "equities earnings-backfill: rows_added={} rows_deduped={} partitions_written={} symbols_with_earnings={} symbols_failed={}",
        total_added, total_deduped, total_partitions, symbols_with_earnings, errors.len()
    );
    Ok(())
}

pub async fn run_earnings_migrate(args: EarningsMigrateArgs) -> Result<()> {
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    let mut symbols_migrated = 0usize;

    for symbol in &args.symbols {
        let v1_dir = args
            .dataset
            .join(&args.venue)
            .join("earnings")
            .join("v1")
            .join(format!("symbol={symbol}"));
        let years = match read_partition_years(&v1_dir) {
            Ok(y) => y,
            Err(e) => {
                tracing::warn!(symbol, dir = %v1_dir.display(), error = %e, "no v1 partitions; skipping");
                continue;
            }
        };
        if years.is_empty() {
            tracing::warn!(symbol, "no v1 year partitions found; skipping");
            continue;
        }
        let mut v2_rows: Vec<earnings::v2::Event> = Vec::new();
        for year in years {
            let v1_rows = ds
                .read::<earnings::v1::Event>(&args.venue, Some(symbol), PartitionTime::Yearly(year))
                .with_context(|| format!("read v1 earnings {symbol} year={year}"))?;
            for r in v1_rows {
                v2_rows.push(earnings::v2::Event {
                    symbol: r.symbol,
                    earnings_date: r.earnings_date,
                    // No source can supply historical timing for these
                    // legacy rows; mark explicitly unknown.
                    session: earnings::v2::Session::Unknown,
                    session_confirmed: None,
                    // Preserve original provenance (where the date came
                    // from) and fetch time; only re-stamp the version.
                    meta: Meta::new(
                        earnings::v2::SCHEMA_VERSION,
                        r.meta.fetched_at,
                        &r.meta.source,
                    ),
                });
            }
        }
        if v2_rows.is_empty() {
            continue;
        }
        let stats = ds
            .write::<earnings::v2::Event>(&args.venue, Some(symbol), &v2_rows)
            .with_context(|| format!("Dataset::write v2 earnings (migrate) for {symbol}"))?;
        tracing::info!(
            symbol,
            gaps_filled = stats.rows_added,
            already_timed = stats.rows_deduped,
            "migrated v1→v2"
        );
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
        symbols_migrated += 1;
    }
    println!(
        "equities earnings-migrate: rows_added={} (gaps filled as unknown) rows_deduped={} (already timed in v2) partitions_written={} symbols_migrated={}",
        total_added, total_deduped, total_partitions, symbols_migrated
    );
    Ok(())
}

/// Enumerate the `year=YYYY` values present in a yearly+symbol-keyed
/// partition directory (`.../symbol=X/year=YYYY.parquet`).
fn read_partition_years(dir: &std::path::Path) -> std::io::Result<Vec<i32>> {
    let mut years = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("year=") {
            if let Some(y) = rest.strip_suffix(".parquet") {
                if let Ok(year) = y.parse::<i32>() {
                    years.push(year);
                }
            }
        }
    }
    years.sort_unstable();
    Ok(years)
}

pub async fn run_corp_actions(args: CorpActionsArgs) -> Result<()> {
    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        source_label: args.source.clone(),
        user_agent: args.user_agent.clone(),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let meta = Meta::new(
        schema_corp_actions::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    let start_unix = parse_ymd_to_unix(&args.start)?;
    // End-exclusive YYYY-MM-DD turns into "midnight of end day +
    // 1 day" so events on `end` itself are included.
    let end_unix = parse_ymd_to_unix(&args.end)?
        .checked_add(86_400)
        .context("end date overflow")?;

    tracing::info!(
        symbols = args.symbols.len(),
        start = args.start,
        end = args.end,
        "fetching Yahoo corp-actions"
    );

    let mut by_symbol: BTreeMap<String, Vec<schema_corp_actions::v1::Action>> = BTreeMap::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    for symbol in &args.symbols {
        match yahoo_corp_actions::fetch_corp_actions(
            &client,
            &cfg,
            &args.base_url,
            symbol,
            start_unix,
            end_unix,
            &meta,
        )
        .await
        {
            Ok(rows) => {
                tracing::info!(symbol, rows = rows.len(), "decoded");
                by_symbol.entry(symbol.clone()).or_default().extend(rows);
            }
            Err(e) => {
                tracing::warn!(symbol, error = %e, "fetch failed; continuing");
                errors.push((symbol.clone(), format_eq_error(&e)));
            }
        }
        if args.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.rate_limit_ms)).await;
        }
    }

    if by_symbol.values().all(|v| v.is_empty()) {
        if !errors.is_empty() {
            anyhow::bail!(
                "all {} symbol(s) failed; first error: {}",
                errors.len(),
                errors.first().map(|(_, e)| e.as_str()).unwrap_or("?")
            );
        }
        println!(
            "equities corp-actions: rows_added=0 rows_deduped=0 partitions_written=0 (empty)"
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, rows) in &by_symbol {
        if rows.is_empty() {
            continue;
        }
        let stats = ds
            .write::<schema_corp_actions::v1::Action>(&args.venue, Some(symbol), rows)
            .with_context(|| format!("Dataset::write yahoo corp_actions for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "equities corp-actions: rows_added={} rows_deduped={} partitions_written={} symbols_failed={}",
        total_added, total_deduped, total_partitions, errors.len()
    );
    Ok(())
}

fn parse_ymd_to_unix(s: &str) -> Result<i64> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {s}"))?;
    let dt = chrono::Utc
        .from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
    Ok(dt.timestamp())
}

fn format_eq_error(e: &EqFetchError) -> String {
    e.to_string()
}
