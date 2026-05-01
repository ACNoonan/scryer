//! `scry solana drift-liquidations` — Drift Protocol liquidation
//! event panel.
//!
//! Same architectural shape as `scry solana kamino-liquidations`:
//! sig-paginate via proxy → stage 2 (Helius parseTransactions OR
//! proxy-routed getTransaction) → IX decode → write per-day no-key
//! partitions.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_solana::drift_liquidations::{extract_liquidations, DRIFT_PROGRAM};
use scryer_fetch_solana::get_transactions::{get_transactions_via_proxy, GetTxConfig};
use scryer_fetch_solana::sig_paginate::{get_signatures_in_window, SigPaginateConfig};
use scryer_fetch_solana::{parse_all, ParseTxsConfig};
use scryer_schema::drift_liquidation::v1 as schema;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct DriftLiquidationsArgs {
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    #[arg(long, env = "HELIUS_API_KEY", default_value = "")]
    helius_api_key: String,
    /// Switch stage 2 from Helius `parseTransactions` to proxy-routed
    /// `getTransaction`. Slower but quota-resilient.
    #[arg(long)]
    use_get_transaction: bool,
    /// Window start as `YYYY-MM-DD` UTC.
    #[arg(long)]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC.
    #[arg(long)]
    end: String,
    #[arg(long, default_value = "helius:parseTransactions")]
    source: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::DRIFT)]
    venue: String,
}

pub async fn run_drift_liquidations(args: DriftLiquidationsArgs) -> Result<()> {
    if !args.use_get_transaction && args.helius_api_key.is_empty() {
        anyhow::bail!(
            "HELIUS_API_KEY required (pass --helius-api-key, set in .env, or use --use-get-transaction)"
        );
    }
    let helius_parse_url = format!(
        "https://api.helius.xyz/v0/transactions?api-key={}",
        args.helius_api_key
    );

    let now = Utc::now();
    let meta = Meta::new(schema::SCHEMA_VERSION, now.timestamp(), &args.source);

    let start_ts = parse_unix_ts(&args.start)?;
    let end_ts = parse_unix_ts(&args.end)? + 86_400;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;
    let paginate_cfg = SigPaginateConfig::default();
    let parse_cfg = ParseTxsConfig::default();

    tracing::info!(
        program = DRIFT_PROGRAM,
        start = args.start,
        end = args.end,
        "stage 1: paginating signatures against Drift program"
    );
    let sigs = get_signatures_in_window(
        &client,
        &args.proxy_url,
        DRIFT_PROGRAM,
        start_ts,
        end_ts,
        &paginate_cfg,
    )
    .await
    .context("sig pagination")?;
    tracing::info!(count = sigs.len(), "sig pagination complete");

    if sigs.is_empty() {
        println!("drift_liquidations: rows_added=0 (empty window)");
        return Ok(());
    }
    let sig_strs: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();

    let txs = if args.use_get_transaction {
        tracing::info!(sigs = sig_strs.len(), "stage 2: getTransaction (proxy)");
        let cfg = GetTxConfig::default();
        get_transactions_via_proxy(&client, &args.proxy_url, &sig_strs, &cfg)
            .await
            .context("getTransaction")?
    } else {
        tracing::info!(
            sigs = sig_strs.len(),
            batch_size = parse_cfg.batch_size,
            "stage 2: parseTransactions"
        );
        parse_all(&client, &helius_parse_url, &sig_strs, &parse_cfg)
            .await
            .context("parseTransactions")?
    };
    tracing::info!(parsed = txs.len(), "stage 2 complete");

    let mut rows: Vec<schema::Liquidation> = Vec::new();
    for tx in &txs {
        rows.extend(extract_liquidations(tx, &meta));
    }
    tracing::info!(
        liquidations = rows.len(),
        sigs_scanned = sig_strs.len(),
        "decode complete"
    );

    if rows.is_empty() {
        println!(
            "drift_liquidations: rows_added=0 sigs_scanned={} (no liquidations decoded)",
            sig_strs.len()
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<schema::Liquidation>(&args.venue, None, &rows)
        .context("Dataset::write drift_liquidations")?;
    let mut by_type: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for r in &rows {
        *by_type.entry(r.liquidation_type.clone()).or_insert(0) += 1;
    }
    println!(
        "drift_liquidations: rows_added={} rows_deduped={} partitions_written={} sigs_scanned={} per_type={:?}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        sig_strs.len(),
        by_type
    );
    Ok(())
}

fn parse_unix_ts(s: &str) -> Result<i64> {
    use chrono::{NaiveDate, TimeZone};
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {s}"))?;
    let dt = Utc
        .from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
    Ok(dt.timestamp())
}
