//! `scry equity-options iv-snapshot` — single-stock ATM IV capture.
//!
//! Fetches one ATM-IV reading per requested symbol at the front-week
//! expiry > capture_ts + 7 days, writing one
//! `volatility.<venue>.single_stock_iv.v2` row per symbol. Daily
//! cadence is enough for the consumer (analysis filters to Friday
//! rows); the manifest sensor is `interval(86400s)`.
//!
//! Wishlist item 52, methodology lock
//! "Single-Stock IV Schema - 2026-05-02".

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use scryer_fetch_equity_options::yahoo::{
    self, bootstrap_session, fetch_atm_iv, PollConfig as YahooPollConfig,
    DEFAULT_BASE_URL as YAHOO_BASE, SOURCE_LABEL as YAHOO_SOURCE,
};
use scryer_schema::single_stock_iv;
use scryer_store::{venue, Dataset};

/// Closed venue enum. Today only `yahoo` is supported; paid venues
/// (`tradier`, `optionmetrics`, `cboe`) land as separate variants
/// when their fetcher modules are added.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Venue {
    Yahoo,
}

impl Venue {
    fn dataset_venue(self) -> &'static str {
        match self {
            Self::Yahoo => venue::VOLATILITY_YAHOO,
        }
    }
}

/// Default Paper-1 universe per methodology lock. GLD/TLT are
/// nice-to-have and left to operator-side `--symbols` when wanted.
pub const DEFAULT_SYMBOLS: &str = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,MSTR,HOOD";

#[derive(Parser, Debug)]
pub struct IvSnapshotArgs {
    /// Comma-separated symbols. Default: Paper-1 universe.
    #[arg(long, value_delimiter = ',', default_value = DEFAULT_SYMBOLS)]
    symbols: Vec<String>,
    /// Source venue. Closed enum; `yahoo` is the only free venue.
    #[arg(long, value_enum, default_value_t = Venue::Yahoo)]
    source: Venue,
    /// Override the row's `_source` label. Default for `yahoo` is
    /// `yahoo:options:v7`. Manifests append `:runner` to attribute
    /// runner-driven fires (e.g. `yahoo:options:v7:runner`).
    #[arg(long)]
    source_label: Option<String>,
    #[arg(long)]
    base_url: Option<String>,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-symbol pacing in milliseconds. Yahoo doesn't publish a
    /// rate limit but staggers prevent IP-level throttling.
    #[arg(long, default_value_t = 500)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
}

pub async fn run_iv_snapshot(args: IvSnapshotArgs) -> Result<()> {
    if args.symbols.is_empty() {
        anyhow::bail!("--symbols cannot be empty");
    }
    match args.source {
        Venue::Yahoo => run_yahoo(args).await,
    }
}

async fn run_yahoo(args: IvSnapshotArgs) -> Result<()> {
    let cfg = YahooPollConfig {
        base_url: args.base_url.clone().unwrap_or_else(|| YAHOO_BASE.to_string()),
        source_label: args.source_label.clone().unwrap_or_else(|| YAHOO_SOURCE.to_string()),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
        ..Default::default()
    };
    let session = bootstrap_session(&cfg)
        .await
        .context("yahoo session bootstrap (cookie + crumb)")?;
    tracing::info!(crumb_len = session.crumb.len(), "yahoo session bootstrapped");

    let now = Utc::now();
    let capture_ts = now.timestamp();
    let fetched_at = capture_ts;

    let mut rows: Vec<single_stock_iv::v2::SingleStockIv> = Vec::with_capacity(args.symbols.len());
    let mut failures: Vec<(String, String)> = Vec::new();
    for (i, sym_in) in args.symbols.iter().enumerate() {
        let sym = sym_in.trim().to_uppercase();
        if sym.is_empty() {
            continue;
        }
        if i > 0 && cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
        match fetch_atm_iv(&session, &cfg, &sym, capture_ts, fetched_at).await {
            Ok(row) => {
                tracing::info!(symbol = %sym, atm_iv = row.atm_iv, dte = row.days_to_expiry, "yahoo iv ok");
                rows.push(row);
            }
            Err(e) => {
                tracing::warn!(symbol = %sym, error = %e, "yahoo iv failed");
                failures.push((sym, e.to_string()));
            }
        }
    }

    if rows.is_empty() {
        if failures.is_empty() {
            println!("equity-options iv-snapshot: rows_added=0 (no symbols processed)");
            return Ok(());
        }
        // All symbols failed — surface the failure so the runner
        // marks this fire as failed rather than silent-empty.
        anyhow::bail!(
            "all {} symbol(s) failed: {:?}",
            failures.len(),
            failures
        );
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<single_stock_iv::v2::SingleStockIv>(args.source.dataset_venue(), None, &rows)
        .with_context(|| format!("Dataset::write venue={}", args.source.dataset_venue()))?;

    println!(
        "equity-options iv-snapshot: rows_added={} rows_deduped={} partitions_written={} symbols_ok={} symbols_failed={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        rows.len(),
        failures.len(),
    );
    if !failures.is_empty() {
        eprintln!("partial failure: {:?}", failures);
    }
    // Silence unused-import lint when adding more venues later.
    let _ = yahoo::MIN_HORIZON_SECS;
    Ok(())
}
