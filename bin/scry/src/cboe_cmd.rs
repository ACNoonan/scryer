//! `scry cboe indices` — VIX-family + SKEW historical-bar fetcher.
//!
//! Pulls the FULL public history from CBOE's `cdn.cboe.com` CSV
//! endpoints. Re-runs are no-ops via `_dedup_key` — schedule
//! weekly via launchd to keep recent days fresh.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_cboe::{
    fetch_index_history, PollConfig, DEFAULT_BASE_URL, SOURCE_LABEL, SUPPORTED_INDICES,
};
use scryer_schema::cboe_indices;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct IndicesArgs {
    /// Comma-separated CBOE index identifiers. Defaults to the
    /// 6-index VIX-family + SKEW bundle: VIX,VIX9D,VIX1D,VIX3M,VIX6M,SKEW.
    #[arg(long, value_delimiter = ',')]
    indices: Vec<String>,
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
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CBOE)]
    venue: String,
}

pub async fn run_indices(args: IndicesArgs) -> Result<()> {
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
    let indices: Vec<String> = if args.indices.is_empty() {
        SUPPORTED_INDICES.iter().map(|s| s.to_string()).collect()
    } else {
        args.indices.clone()
    };

    let mut all_rows: Vec<cboe_indices::v1::Bar> = Vec::new();
    let mut per_index: BTreeMap<String, usize> = BTreeMap::new();
    for idx in &indices {
        let rows = fetch_index_history(&client, &cfg, idx, fetched_at)
            .await
            .with_context(|| format!("fetch_index_history {idx}"))?;
        per_index.insert(idx.clone(), rows.len());
        tracing::info!(index = %idx, rows = rows.len(), "decoded");
        all_rows.extend(rows);
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }

    if all_rows.is_empty() {
        println!("cboe indices: rows_added=0 (empty across all indices)");
        return Ok(());
    }

    // Bucket by index for the partition-key write.
    let mut by_index: BTreeMap<String, Vec<cboe_indices::v1::Bar>> = BTreeMap::new();
    for r in all_rows {
        by_index.entry(r.index.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (idx, rows) in &by_index {
        let stats = ds
            .write::<cboe_indices::v1::Bar>(&args.venue, Some(idx), rows)
            .with_context(|| format!("Dataset::write index={idx}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }

    println!(
        "cboe indices: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_index={per_index:?}"
    );
    Ok(())
}
