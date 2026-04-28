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
    fetch_body, nasdaq_halts, PollConfig,
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
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "nasdaq:rss")]
    source: String,
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
