//! `scry fred macro-calendar` — FRED macro release-calendar fetcher.
//!
//! Calls FRED's `/release/dates` for each release in the default
//! 6-release set (CPI / NFP / GDP / PCE / PPI / RetailSales) over the
//! requested `[start, end]` window. Writes one
//! `fred_macro.v1::Event` row per (release, date) pair to
//! `dataset/fred/macro_calendar/v1/year=YYYY.parquet`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use clap::Parser;
use scryer_fetch_fred::{
    fetch_default_calendar, fetch_release_dates, release::lookup as lookup_release,
    release::ReleaseEntry, PollConfig, DEFAULT_BASE_URL,
};
use scryer_schema::{fred_macro, Meta};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct MacroCalendarArgs {
    /// Window start as `YYYY-MM-DD`. Default: 1 year ago.
    #[arg(long, default_value = "")]
    start: String,
    /// Window end as `YYYY-MM-DD`. Default: 1 year ahead.
    #[arg(long, default_value = "")]
    end: String,
    /// FRED API key. Defaults to the `FRED_API_KEY` env var (loaded
    /// from `./.env` via dotenvy). Register for free at
    /// `https://fredaccount.stlouisfed.org/apikey`.
    #[arg(long, env = "FRED_API_KEY")]
    api_key: String,
    /// Override the default 6-release set with a comma-separated
    /// list of FRED release IDs. Unknown IDs get
    /// `event_name = "release_<id>"` and the upstream's
    /// `release_name`.
    #[arg(long, value_delimiter = ',')]
    release_ids: Vec<i32>,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "fred:release_dates")]
    source: String,
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Delay between successive release calls in milliseconds.
    /// FRED's free tier is 120 calls/min; default is gentle.
    #[arg(long, default_value_t = 500)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::FRED)]
    venue: String,
}

pub async fn run_macro_calendar(args: MacroCalendarArgs) -> Result<()> {
    if args.api_key.is_empty() {
        anyhow::bail!(
            "FRED API key required; pass --api-key or set FRED_API_KEY env var. Register free at https://fredaccount.stlouisfed.org/apikey"
        );
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
    let start = if args.start.is_empty() {
        (now - chrono::Duration::days(365)).format("%Y-%m-%d").to_string()
    } else {
        args.start.clone()
    };
    let end = if args.end.is_empty() {
        (now + chrono::Duration::days(365)).format("%Y-%m-%d").to_string()
    } else {
        args.end.clone()
    };

    let meta = Meta::new(fred_macro::v1::SCHEMA_VERSION, now.timestamp(), &args.source);

    tracing::info!(
        start = start,
        end = end,
        custom_release_ids = args.release_ids.len(),
        "fetching FRED macro calendar"
    );

    let events = if args.release_ids.is_empty() {
        fetch_default_calendar(&client, &cfg, &args.api_key, &start, &end, &meta)
            .await
            .context("fetch_default_calendar")?
    } else {
        let mut out = Vec::new();
        for id in &args.release_ids {
            let entry: ReleaseEntry = match lookup_release(*id) {
                Some(e) => *e,
                None => {
                    let name: &'static str = Box::leak(format!("release_{id}").into_boxed_str());
                    let upstream: &'static str = Box::leak(format!("FRED release {id}").into_boxed_str());
                    ReleaseEntry {
                        release_id: *id,
                        event_name: name,
                        upstream_name: upstream,
                    }
                }
            };
            let rows = fetch_release_dates(
                &client,
                &cfg,
                &args.api_key,
                &entry,
                &start,
                &end,
                &meta,
            )
            .await
            .with_context(|| format!("fetch release {id}"))?;
            tracing::info!(release_id = id, rows = rows.len(), "decoded");
            out.extend(rows);
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        out
    };

    if events.is_empty() {
        println!("fred macro-calendar: rows_added=0 (empty window)");
        return Ok(());
    }

    // The schema is yearly + no-key, so Dataset::write handles the
    // year bucketing internally; just hand it the full vec.
    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<fred_macro::v1::Event>(&args.venue, None, &events)
        .context("Dataset::write")?;

    // Per-event-name summary for the operator log.
    let mut by_name: BTreeMap<String, usize> = BTreeMap::new();
    for ev in &events {
        *by_name.entry(ev.event_name.clone()).or_insert(0) += 1;
    }
    let by_year_min = events.iter().map(|e| date32_to_year(e.event_date)).min().unwrap_or(0);
    let by_year_max = events.iter().map(|e| date32_to_year(e.event_date)).max().unwrap_or(0);
    println!(
        "fred macro-calendar: rows_added={} rows_deduped={} partitions_written={} year_range={}-{} per_event={:?}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        by_year_min,
        by_year_max,
        by_name,
    );
    Ok(())
}

fn date32_to_year(date32: i32) -> i32 {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (epoch + chrono::Duration::days(date32 as i64)).year()
}
