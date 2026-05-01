//! `scry sec edgar-8k` — SEC EDGAR 8-K filing fetcher.
//!
//! Resolves tickers → CIKs via SEC's `company_tickers.json`, then
//! pulls each CIK's `submissions/CIK*.json` and emits one
//! `edgar_8k.v1::Filing` row per 8-K (or 8-K/A) filing.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_sec::{
    fetch_8k_filings, fetch_company_tickers, FetchError, PollConfig, COMPANY_TICKERS_URL,
    SOURCE_LABEL, SUBMISSIONS_BASE_URL,
};
use scryer_schema::edgar_8k;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct Edgar8kArgs {
    /// Comma-separated tickers to fetch. Default: the 10 xStock
    /// underliers + headline crypto-correlated names.
    #[arg(long, value_delimiter = ',', default_value = "TSLA,AAPL,GOOGL,NVDA,MSTR,HOOD,COIN,RBLX,SPY,QQQ")]
    tickers: Vec<String>,
    /// Override individual ticker→CIK lookups; useful when a
    /// ticker isn't in `company_tickers.json` (e.g., delisted).
    /// Format: `TICKER:CIK,TICKER:CIK,...` (CIK must be 10 digits
    /// or will be zero-padded).
    #[arg(long, value_delimiter = ',')]
    cik_overrides: Vec<String>,
    /// User-Agent header (SEC fair-access policy requires
    /// `Name email@example.com` form).
    #[arg(long, env = "SCRYER_SEC_UA", default_value = "scryer scryer@local")]
    user_agent: String,
    #[arg(long, default_value = SOURCE_LABEL)]
    source: String,
    #[arg(long, default_value = SUBMISSIONS_BASE_URL)]
    base_url: String,
    #[arg(long, default_value = COMPANY_TICKERS_URL)]
    company_tickers_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value_t = 200)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::SEC)]
    venue: String,
}

pub async fn run_edgar_8k(args: Edgar8kArgs) -> Result<()> {
    if args.tickers.is_empty() {
        anyhow::bail!("--tickers cannot be empty");
    }
    let cfg = PollConfig {
        submissions_base_url: args.base_url.clone(),
        company_tickers_url: args.company_tickers_url.clone(),
        user_agent: args.user_agent.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;
    let now = Utc::now();
    let meta = Meta::new(
        edgar_8k::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    tracing::info!("loading SEC company_tickers.json");
    let mut tk2cik = fetch_company_tickers(&client, &cfg)
        .await
        .context("fetch_company_tickers")?;
    tracing::info!(loaded = tk2cik.len(), "ticker→CIK map ready");

    // Apply manual overrides.
    for entry in &args.cik_overrides {
        let mut parts = entry.splitn(2, ':');
        if let (Some(t), Some(cik)) = (parts.next(), parts.next()) {
            let cik_padded = format!("{:0>10}", cik.trim_start_matches(|c: char| c == '0'));
            tk2cik.insert(t.to_uppercase(), cik_padded);
        }
    }

    let mut all_rows: Vec<edgar_8k::v1::Filing> = Vec::new();
    let mut per_ticker: BTreeMap<String, usize> = BTreeMap::new();
    for tkr in &args.tickers {
        let upper = tkr.to_uppercase();
        let cik = match tk2cik.get(&upper) {
            Some(c) => c.clone(),
            None => {
                tracing::warn!(ticker = %upper, "not in company_tickers.json; skipping (use --cik-overrides to force)");
                continue;
            }
        };
        match fetch_8k_filings(&client, &cfg, &cik, &upper, &meta).await {
            Ok(rows) => {
                tracing::info!(ticker = %upper, cik = %cik, rows = rows.len(), "decoded");
                per_ticker.insert(upper.clone(), rows.len());
                all_rows.extend(rows);
            }
            Err(FetchError::UpstreamStatus { status, body_head }) => {
                tracing::warn!(ticker = %upper, status, body_head, "fetch failed; continuing");
            }
            Err(e) => {
                tracing::warn!(ticker = %upper, error = %e, "fetch failed; continuing");
            }
        }
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }

    if all_rows.is_empty() {
        println!("sec edgar-8k: rows_added=0 (no 8-Ks decoded across tickers)");
        return Ok(());
    }

    let mut by_ticker: BTreeMap<String, Vec<edgar_8k::v1::Filing>> = BTreeMap::new();
    for r in all_rows {
        by_ticker.entry(r.ticker.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (tkr, rows) in &by_ticker {
        let stats = ds
            .write::<edgar_8k::v1::Filing>(&args.venue, Some(tkr), rows)
            .with_context(|| format!("Dataset::write ticker={tkr}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "sec edgar-8k: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_ticker={per_ticker:?}"
    );
    Ok(())
}
