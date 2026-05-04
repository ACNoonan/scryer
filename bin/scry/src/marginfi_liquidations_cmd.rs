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
    derive_vault_authority, extract_liquidations, BankInfo, BankRegistry,
    INSURANCE_VAULT_AUTH_SEED, MARGINFI_PROGRAM_ID,
};
use scryer_fetch_solana::sig_paginate::{get_signatures_in_window, SigPaginateConfig};
use scryer_schema::marginfi_liquidation::v1 as schema;
use scryer_schema::Meta;
use scryer_store::{read_marginfi_bank_registry, venue, Dataset};
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
    /// Conflicts with `--bank-registry-from-parquet`. Useful for
    /// hand-curated overrides; for the canonical case prefer the
    /// parquet path.
    #[arg(long, conflicts_with = "bank_registry_from_parquet")]
    bank_registry_json: Option<PathBuf>,
    /// Optional path to a `marginfi_reserve.v1` parquet file or
    /// directory tree. The latest snapshot per bank is read and used
    /// to enrich each liquidation row's `asset_symbol` / `asset_decimals`
    /// / `asset_oracle` (same for liab). When neither this flag nor
    /// `--bank-registry-json` is set, the CLI auto-discovers the
    /// canonical partition at `<dataset>/<venue>/reserves/v1/` and
    /// uses it when present (with a one-line warn if missing).
    #[arg(long)]
    bank_registry_from_parquet: Option<PathBuf>,
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
    #[serde(default)]
    insurance_vault_authority: String,
}

fn load_bank_registry_from_json(path: &PathBuf) -> Result<BankRegistry> {
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
                insurance_vault_authority: e.insurance_vault_authority,
            },
        );
    }
    Ok(reg)
}

fn load_bank_registry_from_parquet(path: &PathBuf) -> Result<BankRegistry> {
    let entries = read_marginfi_bank_registry(path)
        .with_context(|| format!("reading marginfi_reserve.v1 parquet at {}", path.display()))?;
    let mut reg = BankRegistry::new();
    let mut authorities_derived = 0usize;
    for e in &entries {
        let insurance_vault_authority = match e.insurance_vault_authority_bump {
            Some(bump) => derive_vault_authority(&e.bank, INSURANCE_VAULT_AUTH_SEED, bump)
                .unwrap_or_default(),
            None => String::new(),
        };
        if !insurance_vault_authority.is_empty() {
            authorities_derived += 1;
        }
        reg.insert(
            e.bank.clone(),
            BankInfo {
                mint: e.asset_mint.clone(),
                mint_decimals: e.asset_decimals,
                mint_symbol: e.asset_symbol.clone(),
                oracle: e.primary_oracle.clone(),
                insurance_vault_authority,
            },
        );
    }
    tracing::info!(
        path = %path.display(),
        banks_loaded = entries.len(),
        insurance_vault_authorities_derived = authorities_derived,
        "loaded bank registry from marginfi_reserve.v1 parquet"
    );
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

    let bank_registry = if let Some(p) = &args.bank_registry_json {
        load_bank_registry_from_json(p)?
    } else if let Some(p) = &args.bank_registry_from_parquet {
        load_bank_registry_from_parquet(p)?
    } else {
        let auto = args.dataset.join(&args.venue).join("reserves").join("v1");
        if auto.exists() {
            load_bank_registry_from_parquet(&auto)?
        } else {
            tracing::warn!(
                expected = %auto.display(),
                "no bank registry source given and canonical partition is absent — \
                 rows will ship with asset_symbol=\"?\" / asset_oracle=\"\""
            );
            BankRegistry::new()
        }
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
