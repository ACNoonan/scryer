//! `scry solana marginfi-reserves` — one-shot snapshot of MarginFi-v2
//! Bank account configs.
//!
//! Single proxy-routed `getProgramAccounts` against the program ID
//! with a Bank-disc memcmp filter; per-Bank decode + xStock filter.
//! Writes one `marginfi_reserve.v1::Reserve` row per matched Bank to
//! `dataset/marginfi/reserves/v1/year=Y/month=M/day=D.parquet`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::jupiter::XSTOCK_MINTS;
use scryer_fetch_solana::marginfi_reserves::{
    fetch_marginfi_reserves, MarginfiReservesFetcherConfig, MintEntry,
};
use scryer_schema::{marginfi_reserve, Meta};
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct MarginfiReservesArgs {
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Disable the post-decode xStock-mint filter; emit every Bank
    /// (mints not in the registry decode with `asset_symbol = "?"`).
    /// Default mode (xstock-only) keeps only Banks whose mint matches
    /// the canonical 8-xStock set.
    #[arg(long, default_value_t = false)]
    all: bool,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getProgramAccounts")]
    source: String,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    /// Venue under which reserves are written. Defaults to `marginfi`
    /// per the methodology lock.
    #[arg(long, default_value = scryer_store::venue::MARGINFI)]
    venue: String,
}

pub async fn run_marginfi_reserves(args: MarginfiReservesArgs) -> Result<()> {
    let mints: Vec<MintEntry> = XSTOCK_MINTS
        .iter()
        .map(|(s, m)| MintEntry {
            symbol: s.to_string(),
            mint: m.to_string(),
        })
        .collect();

    let cfg = MarginfiReservesFetcherConfig::new(args.proxy_url.clone());
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let meta = Meta::new(
        marginfi_reserve::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    tracing::info!(
        proxy = args.proxy_url,
        xstock_only = !args.all,
        registry_size = mints.len(),
        "snapshotting MarginFi-v2 Bank reserves"
    );

    let summary = fetch_marginfi_reserves(&client, &cfg, &mints, !args.all, &meta)
        .await
        .context("fetch_marginfi_reserves")?;

    tracing::info!(
        returned = summary.returned_accounts,
        decoded = summary.decoded,
        wrong_size = summary.wrong_size,
        wrong_disc = summary.wrong_disc,
        filtered_out = summary.filtered_out,
        decode_errors = summary.decode_errors,
        rows = summary.rows.len(),
        "decode complete"
    );

    if summary.rows.is_empty() {
        println!(
            "marginfi_reserves: rows_added=0 rows_deduped=0 partitions_written=0 (no banks matched the filter)"
        );
        return Ok(());
    }
    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<marginfi_reserve::v1::Reserve>(&args.venue, None, &summary.rows)
        .context("Dataset::write marginfi_reserves")?;
    println!(
        "marginfi_reserves: rows_added={} rows_deduped={} partitions_written={} returned={} decoded={} filtered_out={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        summary.returned_accounts,
        summary.decoded,
        summary.filtered_out
    );
    Ok(())
}
