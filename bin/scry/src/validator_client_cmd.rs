//! `scry solana validator-client` — per-epoch Solana leader→client
//! refresh for `validator_client.v1`. Wishlist 51b.
//!
//! Joins three sources to emit one row per (current_epoch,
//! leader_pubkey):
//!
//! - Solana RPC `getEpochInfo` (current epoch)
//! - Solana RPC `getClusterNodes` (gossip-visible version)
//! - Stakewiz `/validators` (jito-vs-vanilla discriminator)
//!
//! Per-epoch row unit; an hourly cadence is fine because the
//! mapping is constant within an epoch (epochs are ~2 days). A
//! single fire writes ~1500 rows per epoch and the store dedupes
//! against the existing epoch's rows.
//!
//! Output: `dataset/solana_validator/client_label/v1/year=YYYY.parquet`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::validator_client::{
    refresh as refresh_validator_client, RefreshConfig, JITO_KOBE_VALIDATORS_URL,
    STAKEWIZ_VALIDATORS_URL,
};
use scryer_schema::validator_client;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct ValidatorClientArgs {
    /// JSON-RPC endpoint for `getEpochInfo` + `getClusterNodes` —
    /// the local proxy by default.
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Stakewiz validators endpoint. Public, no auth.
    #[arg(long, default_value = STAKEWIZ_VALIDATORS_URL)]
    stakewiz_url: String,

    /// Jito kobe validators endpoint. Public, no auth.
    /// Authoritative for the BAM-vs-jito-agave distinction.
    #[arg(long, default_value = JITO_KOBE_VALIDATORS_URL)]
    jito_kobe_url: String,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getClusterNodes+stakewiz:validators+jito:kobe")]
    source: String,

    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::SOLANA_VALIDATOR)]
    venue: String,
}

pub async fn run_validator_client(args: ValidatorClientArgs) -> Result<()> {
    let cfg = RefreshConfig {
        proxy_rpc_url: args.proxy_url.clone(),
        stakewiz_url: args.stakewiz_url.clone(),
        jito_kobe_url: args.jito_kobe_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let rows = refresh_validator_client(&client, &cfg)
        .await
        .context("validator-client refresh")?;

    if rows.is_empty() {
        println!("validator-client: rows_added=0 (no validators found)");
        return Ok(());
    }

    let n_unknown = rows
        .iter()
        .filter(|r| r.client_label == validator_client::v1::CLIENT_UNKNOWN)
        .count();
    let epoch = rows[0].epoch;

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<validator_client::v1::ClientLabel>(&args.venue, None, &rows)
        .context("Dataset::write validator_client")?;

    let pct_unknown = if rows.is_empty() {
        0.0
    } else {
        100.0 * n_unknown as f64 / rows.len() as f64
    };
    println!(
        "validator-client: epoch={} rows_total={} rows_added={} rows_deduped={} partitions={} unknown_pct={:.1}",
        epoch,
        rows.len(),
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        pct_unknown,
    );
    Ok(())
}
