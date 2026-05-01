//! `scry deribit dvol` — Deribit BTC/ETH DVOL fetcher.
//!
//! Public REST against `www.deribit.com/api/v2`. Default pulls the
//! last 90 days of daily DVOL closes for both BTC and ETH; cron /
//! launchd at the desired cadence (typical: daily at NY close).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_deribit::{
    fetch_dvol, PollConfig, DEFAULT_BASE_URL, RESOLUTION_DAILY_SECS, SOURCE_LABEL,
};
use scryer_schema::deribit_iv;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct DvolArgs {
    /// Comma-separated currencies. Default: BTC,ETH.
    #[arg(long, value_delimiter = ',', default_value = "BTC,ETH")]
    currencies: Vec<String>,
    /// Lookback window in days. Default 90 days = generous coverage
    /// for daily polling that's tolerant of multi-day downtime.
    #[arg(long, default_value_t = 90)]
    lookback_days: i64,
    /// Resolution in seconds (Deribit accepts 60, 600, 1800, 3600,
    /// 21600, 43200, 86400). Default daily.
    #[arg(long, default_value_t = RESOLUTION_DAILY_SECS)]
    resolution_secs: u64,
    #[arg(long, default_value = SOURCE_LABEL)]
    source: String,
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::DERIBIT)]
    venue: String,
}

pub async fn run_dvol(args: DvolArgs) -> Result<()> {
    if args.currencies.is_empty() {
        anyhow::bail!("--currencies cannot be empty");
    }
    let cfg = PollConfig {
        base_url: args.base_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let fetched_at = now.timestamp();
    let start_ts = fetched_at - args.lookback_days * 86_400;
    let end_ts = fetched_at;

    let mut all_rows: Vec<deribit_iv::v1::DvolBar> = Vec::new();
    let mut per_currency: BTreeMap<String, usize> = BTreeMap::new();
    for c in &args.currencies {
        let curr = c.to_uppercase();
        let rows = fetch_dvol(
            &client,
            &cfg,
            &curr,
            start_ts,
            end_ts,
            args.resolution_secs,
            fetched_at,
        )
        .await
        .with_context(|| format!("fetch_dvol {curr}"))?;
        per_currency.insert(curr.clone(), rows.len());
        tracing::info!(currency = %curr, rows = rows.len(), "decoded");
        all_rows.extend(rows);
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }

    if all_rows.is_empty() {
        println!("deribit dvol: rows_added=0 (empty response across currencies)");
        return Ok(());
    }

    let mut by_curr: BTreeMap<String, Vec<deribit_iv::v1::DvolBar>> = BTreeMap::new();
    for r in all_rows {
        by_curr.entry(r.underlying.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (curr, rows) in &by_curr {
        let stats = ds
            .write::<deribit_iv::v1::DvolBar>(&args.venue, Some(curr), rows)
            .with_context(|| format!("Dataset::write currency={curr}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }

    println!(
        "deribit dvol: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_currency={per_currency:?}"
    );
    Ok(())
}
