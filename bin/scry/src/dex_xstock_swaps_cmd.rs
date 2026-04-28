//! `scry solana dex-xstock-swaps` — cross-DEX xStock swap-print
//! fetcher.
//!
//! For each requested xStock symbol, scans
//! `getSignaturesForAddress(xstock_mint)` over the time window via
//! the proxy, batches Helius `parseTransactions` for the resulting
//! signatures, and emits one row per (signature, trader, mint)
//! triple where the trader's xStock + counter-mint deltas have
//! opposite signs (i.e. an actual swap, not a transfer).
//!
//! `dex_program` is classified at the tx level — single recognized
//! DEX → that label; multiple → `"aggregator"`; none → `"other"`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::jupiter::XSTOCK_MINTS;
use scryer_fetch_solana::dex_xstock_swaps::{default_registry, extract_swaps};
use scryer_fetch_solana::get_transactions::{get_transactions_via_proxy, GetTxConfig};
use scryer_fetch_solana::sig_paginate::{get_signatures_in_window, SigPaginateConfig};
use scryer_fetch_solana::{parse_all, ParseTxsConfig};
use scryer_schema::dex_xstock_swaps::v1 as schema;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct DexXstockSwapsArgs {
    /// JSON-RPC endpoint for `getSignaturesForAddress` — the local
    /// proxy by default.
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Helius API key (used for `parseTransactions`). Reads from
    /// `HELIUS_API_KEY` in `.env` if not provided. Required UNLESS
    /// `--use-get-transaction` is set (which routes stage 2 through
    /// the proxy and skips Helius entirely).
    #[arg(long, env = "HELIUS_API_KEY", default_value = "")]
    helius_api_key: String,
    /// Switch stage 2 from Helius `parseTransactions` to proxy-routed
    /// `getTransaction(jsonParsed)`. Slower (~5-50 tx/s vs ~100 tx/s
    /// on parseTransactions) but multi-provider quota-resilient via
    /// the proxy's failover. Use when Helius daily quota is
    /// exhausted.
    #[arg(long)]
    use_get_transaction: bool,
    /// Comma-separated list of xStock symbols. Defaults to all 8.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Window start as `YYYY-MM-DD` UTC.
    #[arg(long)]
    start: String,
    /// Window end as `YYYY-MM-DD` UTC.
    #[arg(long)]
    end: String,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "helius:parseTransactions")]
    source: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    /// Venue for output. Default: `dex_xstock` per the methodology
    /// (cross-DEX prints live under their own venue).
    #[arg(long, default_value = venue::DEX_XSTOCK)]
    venue: String,
}

pub async fn run_dex_xstock_swaps(args: DexXstockSwapsArgs) -> Result<()> {
    if !args.use_get_transaction && args.helius_api_key.is_empty() {
        anyhow::bail!(
            "HELIUS_API_KEY required (pass --helius-api-key, set in .env, or use --use-get-transaction)"
        );
    }
    let helius_parse_url = format!(
        "https://api.helius.xyz/v0/transactions?api-key={}",
        args.helius_api_key
    );

    // Resolve target symbols.
    let targets: Vec<(&str, &str)> = if args.symbols.is_empty() {
        XSTOCK_MINTS.iter().copied().collect()
    } else {
        let want: std::collections::HashSet<&str> =
            args.symbols.iter().map(|s| s.as_str()).collect();
        XSTOCK_MINTS
            .iter()
            .copied()
            .filter(|(sym, _)| want.contains(*sym))
            .collect()
    };
    if targets.is_empty() {
        anyhow::bail!(
            "no symbols matched; use one of: {:?}",
            XSTOCK_MINTS.iter().map(|(s, _)| *s).collect::<Vec<_>>()
        );
    }

    let start_ts = parse_unix_ts(&args.start)?;
    let end_ts = parse_unix_ts(&args.end)? + 86_400; // inclusive end-of-day

    let now = Utc::now();
    let meta = Meta::new(schema::SCHEMA_VERSION, now.timestamp(), &args.source);
    let registry = default_registry();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;
    let paginate_cfg = SigPaginateConfig::default();
    let parse_cfg = ParseTxsConfig::default();

    tracing::info!(
        symbols = targets.len(),
        start = args.start,
        end = args.end,
        "scanning cross-DEX xStock swaps"
    );

    // Per-symbol bucket so we write one daily partition per symbol
    // per day (Daily + symbol-keyed schema).
    let mut by_symbol: BTreeMap<String, Vec<schema::Swap>> = BTreeMap::new();
    let mut total_sigs = 0usize;
    let mut total_swaps = 0usize;
    for (symbol, mint) in &targets {
        tracing::info!(symbol, mint, "stage 1: paginating signatures");
        let sigs = get_signatures_in_window(
            &client,
            &args.proxy_url,
            mint,
            start_ts,
            end_ts,
            &paginate_cfg,
        )
        .await
        .with_context(|| format!("sig pagination for {symbol}"))?;
        tracing::info!(symbol, count = sigs.len(), "sig pagination complete");
        total_sigs += sigs.len();

        if sigs.is_empty() {
            continue;
        }
        let sig_strs: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();
        let txs = if args.use_get_transaction {
            tracing::info!(
                symbol,
                sigs = sig_strs.len(),
                "stage 2: getTransaction (proxy-routed)"
            );
            let get_tx_cfg = GetTxConfig::default();
            get_transactions_via_proxy(&client, &args.proxy_url, &sig_strs, &get_tx_cfg)
                .await
                .with_context(|| format!("getTransaction for {symbol}"))?
        } else {
            tracing::info!(
                symbol,
                sigs = sig_strs.len(),
                batch_size = parse_cfg.batch_size,
                "stage 2: parseTransactions batches"
            );
            parse_all(&client, &helius_parse_url, &sig_strs, &parse_cfg)
                .await
                .with_context(|| format!("parseTransactions for {symbol}"))?
        };
        tracing::info!(symbol, parsed = txs.len(), "stage 2 complete");

        for tx in &txs {
            let rows = extract_swaps(tx, &registry, &meta);
            total_swaps += rows.len();
            for row in rows {
                if row.xstock_symbol == *symbol {
                    by_symbol
                        .entry(row.xstock_symbol.clone())
                        .or_default()
                        .push(row);
                }
            }
        }
        tracing::info!(symbol, rows_so_far = by_symbol.get(*symbol).map(|v| v.len()).unwrap_or(0));
    }

    tracing::info!(
        total_sigs,
        total_swaps,
        symbols_with_data = by_symbol.len(),
        "decode complete; writing partitions"
    );

    if by_symbol.values().all(|v| v.is_empty()) {
        println!(
            "dex_xstock_swaps: rows_added=0 partitions_written=0 sigs_scanned={} (no swaps decoded)",
            total_sigs
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, rows) in &by_symbol {
        if rows.is_empty() {
            continue;
        }
        let stats = ds
            .write::<schema::Swap>(&args.venue, Some(symbol), rows)
            .with_context(|| format!("Dataset::write dex_xstock_swaps for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "dex_xstock_swaps: rows_added={} rows_deduped={} partitions_written={} sigs_scanned={}",
        total_added, total_deduped, total_partitions, total_sigs
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
