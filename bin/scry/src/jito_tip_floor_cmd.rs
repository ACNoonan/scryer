//! `scry solana jito-tip-floor` — single-tick Jito tip-floor poll.
//!
//! One call to `https://bundles.jito.wtf/api/v1/bundles/tip_floor`
//! per invocation. Schedule the desired cadence externally via
//! launchd / cron (typical: every 10s). Re-polls within a single
//! upstream rolling-window dedup on the upstream `time` field, so
//! over-polling produces zero redundant rows.
//!
//! Output: `dataset/jito/tip_floor/v1/year=Y/month=M/day=D.parquet`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_jito::tip_floor::{fetch_tip_floor, TipFloorConfig, DEFAULT_BASE_URL, SOURCE_LABEL};
use scryer_schema::jito_tip_floor;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct JitoTipFloorArgs {
    /// `bundles.jito.wtf` base URL. Defaults to the public mainnet
    /// host.
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = SOURCE_LABEL)]
    source: String,

    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::JITO)]
    venue: String,
}

pub async fn run_jito_tip_floor(args: JitoTipFloorArgs) -> Result<()> {
    let cfg = TipFloorConfig {
        base_url: args.base_url.clone(),
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
    let fetched_at = now.timestamp();

    let tick = match fetch_tip_floor(&client, &cfg, fetched_at)
        .await
        .context("fetch_tip_floor")?
    {
        Some(t) => t,
        None => {
            println!("jito tip-floor: rows_added=0 (upstream returned empty array)");
            return Ok(());
        }
    };

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<jito_tip_floor::v1::Tick>(&args.venue, None, &[tick.clone()])
        .context("Dataset::write")?;

    println!(
        "jito tip-floor: rows_added={} rows_deduped={} time={} p50_lamports={} ema_p50_lamports={}",
        stats.rows_added,
        stats.rows_deduped,
        tick.time,
        tick.landed_tips_p50,
        tick.ema_landed_tips_p50,
    );
    Ok(())
}
