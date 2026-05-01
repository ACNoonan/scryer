//! `scry solana xstock-holders` — top-N holders snapshot per xStock
//! mint.
//!
//! For each mint in the configured registry (defaults to the 8
//! Backed-issued xStock mints), calls
//! `getTokenLargestAccounts(mint)` → `getMultipleAccounts` (×2 for
//! owner + owner-program). Writes one
//! `xstock_holders.v1::Holder` row per (mint, top-N token account).
//!
//! Schedule weekly via launchd to track concentration drift + spot
//! new protocol vaults.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::jupiter::{XSTOCK_DECIMALS, XSTOCK_MINTS};
use scryer_fetch_solana::xstock_holders::{fetch_holders, PollConfig};
use scryer_schema::{xstock_holders, Meta};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct XstockHoldersArgs {
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Override the default 8-symbol registry. Pass as
    /// `SYM:MINT,SYM:MINT,...`.
    #[arg(long, value_delimiter = ',')]
    mints: Vec<String>,
    #[arg(long, default_value = "rpc:getTokenLargestAccounts")]
    source: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::XSTOCK)]
    venue: String,
}

pub async fn run_xstock_holders(args: XstockHoldersArgs) -> Result<()> {
    let cfg = PollConfig {
        proxy_rpc_url: args.proxy_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;
    let now = Utc::now();
    let snapshot_unix_ts = now.timestamp();
    let meta = Meta::new(
        xstock_holders::v1::SCHEMA_VERSION,
        snapshot_unix_ts,
        &args.source,
    );

    let registry: Vec<(String, String)> = if args.mints.is_empty() {
        XSTOCK_MINTS
            .iter()
            .map(|(s, m)| (s.to_string(), m.to_string()))
            .collect()
    } else {
        args.mints
            .iter()
            .filter_map(|s| {
                let mut parts = s.splitn(2, ':');
                let sym = parts.next()?.trim().to_string();
                let mint = parts.next()?.trim().to_string();
                if sym.is_empty() || mint.is_empty() {
                    None
                } else {
                    Some((sym, mint))
                }
            })
            .collect()
    };
    if registry.is_empty() {
        anyhow::bail!("no mints to query (registry empty)");
    }

    let mut all_rows: Vec<xstock_holders::v1::Holder> = Vec::new();
    for (sym, mint) in &registry {
        let rows = fetch_holders(
            &client,
            &cfg,
            mint,
            sym,
            snapshot_unix_ts,
            XSTOCK_DECIMALS as i32,
            &meta,
        )
        .await
        .with_context(|| format!("fetch_holders {sym} ({mint})"))?;
        tracing::info!(symbol = %sym, mint = %mint, rows = rows.len(), "decoded");
        all_rows.extend(rows);
    }

    if all_rows.is_empty() {
        println!("xstock holders: rows_added=0 (no holders returned across registry)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<xstock_holders::v1::Holder>(&args.venue, None, &all_rows)
        .context("Dataset::write")?;
    println!(
        "xstock holders: rows_added={} rows_deduped={} partitions_written={} per_mint_rows={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        all_rows.len()
    );
    Ok(())
}
