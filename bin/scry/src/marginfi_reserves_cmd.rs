//! `scry solana marginfi-reserves` — one-shot snapshot of MarginFi-v2
//! Bank account configs.
//!
//! Single proxy-routed `getProgramAccounts` against the program ID
//! with a Bank-disc memcmp filter; per-Bank decode + xStock filter.
//! Writes one `marginfi_reserve.v1::Reserve` row per matched Bank to
//! `dataset/marginfi/reserves/v1/year=Y/month=M/day=D.parquet`.

use std::collections::HashSet;
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
use serde::Deserialize;

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
    /// Optional path to a JSON `Vec<{"symbol": "...", "mint": "..."}>`
    /// extending the built-in xStock registry with additional SPL
    /// symbol → mint mappings. Used to populate `asset_symbol` for
    /// non-xStock mints under `--all` (USDC, SOL, BONK, etc.). The
    /// built-in 8 xStocks always remain in the registry; entries in
    /// the JSON whose mint matches an xStock override the symbol.
    #[arg(long)]
    symbol_map_json: Option<PathBuf>,
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

#[derive(Debug, Deserialize)]
struct SymbolMapEntry {
    symbol: String,
    mint: String,
}

fn load_symbol_map(path: &PathBuf) -> Result<Vec<MintEntry>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading symbol-map JSON {}", path.display()))?;
    let entries: Vec<SymbolMapEntry> =
        serde_json::from_str(&text).context("parsing symbol-map JSON as Vec<{symbol,mint}>")?;
    Ok(entries
        .into_iter()
        .map(|e| MintEntry {
            symbol: e.symbol,
            mint: e.mint,
        })
        .collect())
}

pub async fn run_marginfi_reserves(args: MarginfiReservesArgs) -> Result<()> {
    // Built-in 8-xStock registry, always present.
    let mut mints: Vec<MintEntry> = XSTOCK_MINTS
        .iter()
        .map(|(s, m)| MintEntry {
            symbol: s.to_string(),
            mint: m.to_string(),
        })
        .collect();
    // Merge in the optional caller-supplied symbol map. JSON entries
    // override xStock entries when mints collide (last-write-wins via
    // dedup-by-mint below); this keeps the JSON authoritative for any
    // explicit operator override.
    if let Some(path) = &args.symbol_map_json {
        let extra = load_symbol_map(path)
            .with_context(|| format!("loading symbol map from {}", path.display()))?;
        let mut seen: HashSet<String> = HashSet::new();
        let mut merged: Vec<MintEntry> = Vec::with_capacity(mints.len() + extra.len());
        // JSON entries first so their symbol wins on collision.
        for e in extra.into_iter().chain(mints.into_iter()) {
            if seen.insert(e.mint.clone()) {
                merged.push(e);
            }
        }
        mints = merged;
        tracing::info!(
            symbol_map_path = %path.display(),
            registry_size = mints.len(),
            "merged caller-supplied symbol map with built-in xStock registry"
        );
    }

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
