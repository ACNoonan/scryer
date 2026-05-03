//! `scry solana marginfi-liquidations` — MarginFi-v2 liquidation
//! event panel.
//!
//! Pipeline: `getSignaturesForAddress(MarginFi program)` over
//! `[--start, --end]` → proxy-routed `getTransaction(jsonParsed)` for
//! every sig (logs are required for Anchor event decode, so no Helius
//! `parseTransactions` fallback) → IX disc match + `Program data:`
//! Anchor event decode → write per-day no-key partitions to
//! `dataset/marginfi/liquidations/v1/`.
//!
//! See `methodology_log.md` "MarginFi-v2" + "Anchor Event Decode From
//! Logs" entries for the locked decode contract.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Utc};
use clap::Parser;
use scryer_fetch_solana::get_transactions::{get_transactions_via_proxy, GetTxConfig};
use scryer_fetch_solana::marginfi_liquidations::{
    extract_liquidations, BankInfo, BankRegistry, MARGINFI_PROGRAM_ID,
};
use scryer_fetch_solana::sig_paginate::{get_signatures_in_window, SigPaginateConfig};
use scryer_schema::marginfi_liquidation::v1 as schema;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};
use serde::Deserialize;

#[derive(Parser, Debug)]
pub struct MarginfiLiquidationsArgs {
    /// JSON-RPC endpoint for `getSignaturesForAddress` and
    /// `getTransaction`. The local proxy by default.
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Window start as `YYYY-MM-DD` UTC.
    #[arg(long)]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC (inclusive end-of-day).
    #[arg(long)]
    end: String,
    /// Optional JSON file mapping bank PDAs to `BankInfo`. Shape:
    /// `[{"bank":"...", "mint":"...", "mint_decimals":8, "mint_symbol":"SPYx", "oracle":"..."}, ...]`.
    /// When omitted the registry is empty and rows ship with
    /// `asset_symbol="?"` / `asset_oracle=""` (and the same for
    /// liab); a follow-on phase wires up parquet auto-load from
    /// `marginfi_reserve.v1`.
    #[arg(long)]
    bank_registry_json: Option<PathBuf>,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getTransaction:marginfi-liquidations")]
    source: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    /// Venue under `dataset/`. Default `marginfi`.
    #[arg(long, default_value = venue::MARGINFI)]
    venue: String,
}

#[derive(Deserialize)]
struct BankRegistryEntry {
    bank: String,
    #[serde(default)]
    mint: String,
    #[serde(default)]
    mint_decimals: u8,
    #[serde(default)]
    mint_symbol: String,
    #[serde(default)]
    oracle: String,
}

fn load_bank_registry(path: &PathBuf) -> Result<BankRegistry> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading bank registry JSON {}", path.display()))?;
    let entries: Vec<BankRegistryEntry> =
        serde_json::from_str(&text).context("parsing bank registry JSON")?;
    let mut reg = BankRegistry::new();
    for e in entries {
        reg.insert(
            e.bank,
            BankInfo {
                mint: e.mint,
                mint_decimals: e.mint_decimals,
                mint_symbol: e.mint_symbol,
                oracle: e.oracle,
            },
        );
    }
    Ok(reg)
}

pub async fn run_marginfi_liquidations(args: MarginfiLiquidationsArgs) -> Result<()> {
    let now = Utc::now();
    let meta = Meta::new(schema::SCHEMA_VERSION, now.timestamp(), &args.source);

    let start_ts = parse_unix_ts(&args.start)?;
    let end_ts = parse_unix_ts(&args.end)? + 86_400;
    if end_ts <= start_ts {
        anyhow::bail!("--end ({end_ts}) must be > --start ({start_ts})");
    }

    let bank_registry = match &args.bank_registry_json {
        Some(p) => load_bank_registry(p)?,
        None => BankRegistry::new(),
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;
    let paginate_cfg = SigPaginateConfig::default();

    tracing::info!(
        program = MARGINFI_PROGRAM_ID,
        start = args.start,
        end = args.end,
        "stage 1: paginating signatures against MarginFi-v2 program"
    );
    let sigs = get_signatures_in_window(
        &client,
        &args.proxy_url,
        MARGINFI_PROGRAM_ID,
        start_ts,
        end_ts,
        &paginate_cfg,
    )
    .await
    .context("sig pagination")?;
    tracing::info!(count = sigs.len(), "sig pagination complete");

    if sigs.is_empty() {
        println!("marginfi_liquidations: rows_added=0 (empty window)");
        return Ok(());
    }

    let sig_strs: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();
    tracing::info!(
        sigs = sig_strs.len(),
        "stage 2: getTransaction (proxy-routed; logs required for Anchor event decode)"
    );
    let cfg = GetTxConfig::default();
    let txs = get_transactions_via_proxy(&client, &args.proxy_url, &sig_strs, &cfg)
        .await
        .context("getTransaction")?;
    tracing::info!(parsed = txs.len(), "stage 2 complete");

    let mut rows: Vec<schema::Liquidation> = Vec::new();
    for tx in &txs {
        rows.extend(extract_liquidations(tx, &bank_registry, &meta));
    }
    tracing::info!(
        liquidations = rows.len(),
        sigs_scanned = sig_strs.len(),
        "decode complete"
    );

    if rows.is_empty() {
        println!(
            "marginfi_liquidations: rows_added=0 sigs_scanned={} (no liquidations decoded)",
            sig_strs.len()
        );
        return Ok(());
    }

    let mut by_pair: HashMap<(String, String), usize> = HashMap::new();
    for r in &rows {
        *by_pair
            .entry((r.asset_symbol.clone(), r.liab_symbol.clone()))
            .or_insert(0) += 1;
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<schema::Liquidation>(&args.venue, None, &rows)
        .context("Dataset::write marginfi_liquidations")?;
    println!(
        "marginfi_liquidations: rows_added={} rows_deduped={} partitions_written={} sigs_scanned={} per_pair={:?}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        sig_strs.len(),
        by_pair
    );
    Ok(())
}

fn parse_unix_ts(s: &str) -> Result<i64> {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {s}"))?;
    let dt = Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
    Ok(dt.timestamp())
}
