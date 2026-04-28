//! `scry cex-funding multi` — multi-venue perp funding-rate fetcher.
//!
//! One subcommand polls four perp venues and writes one
//! `cex_perp_funding_multi.v1::Rate` row per (exchange, symbol,
//! funding_ts) triple. Per-venue switches let the operator subset.
//!
//! Output: `dataset/cex_perp_funding/funding/v1/symbol={SYM}/year=Y/
//! month=M/day=D.parquet`. The `exchange` column lives inside the row
//! (not the partition path), so OKX's BTC + Hyperliquid's BTC stack in
//! the same parquet — partition-key=symbol gives downstream consumers
//! a single-pole partition prune for cross-venue analysis.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_cex_perps::{
    build_client, coinbase_intl, dydx_v4, hyperliquid, okx, PollConfig,
};
use scryer_schema::cex_perp_funding_multi::v1::Rate;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct MultiArgs {
    /// Comma-separated canonical short symbols to poll. Default: BTC,
    /// ETH, SOL — the three with the highest cross-venue liquidity.
    /// Add `BNB`/`XRP`/`DOGE` etc. as upstream support permits; not
    /// every venue lists every symbol.
    #[arg(long, value_delimiter = ',', default_value = "BTC,ETH,SOL")]
    symbols: Vec<String>,
    /// Disable OKX. Default: included.
    #[arg(long, default_value_t = false)]
    no_okx: bool,
    /// Disable Coinbase International. Default: included.
    #[arg(long, default_value_t = false)]
    no_coinbase_intl: bool,
    /// Disable Hyperliquid. Default: included.
    #[arg(long, default_value_t = false)]
    no_hyperliquid: bool,
    /// Disable dYdX v4. Default: included.
    #[arg(long, default_value_t = false)]
    no_dydx_v4: bool,
    /// OKX page-size cap (max 100 per call). Each call returns up to
    /// `limit` most-recent funding records.
    #[arg(long, default_value_t = 100)]
    okx_limit: u32,
    /// Coinbase International page-size cap (max 100 per call).
    #[arg(long, default_value_t = 100)]
    coinbase_limit: u32,
    /// Hyperliquid lookback window in hours. The endpoint requires a
    /// `startTime` (ms unix); we compute it as `now - hours * 3600`.
    /// Default: 168 (7 days at 1h cadence = ~168 rows).
    #[arg(long, default_value_t = 168)]
    hyperliquid_hours: u32,
    /// Per-venue HTTP timeout (seconds).
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    /// Per-venue retry max.
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    /// Per-venue retry delay (seconds).
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-call delay between consecutive symbols within a venue
    /// (milliseconds). Default 250ms keeps us well under every venue's
    /// documented rate limit.
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: std::path::PathBuf,
    #[arg(long, default_value = venue::CEX_PERP_FUNDING)]
    venue: String,
}

pub async fn run_multi(args: MultiArgs) -> Result<()> {
    if args.symbols.is_empty() {
        anyhow::bail!("--symbols cannot be empty");
    }
    let cfg = PollConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
        ..Default::default()
    };
    let client = build_client(&cfg).context("building reqwest client")?;
    let now = Utc::now();
    let fetched_at = now.timestamp();
    let now_ms = now.timestamp_millis();
    let hl_start_ms = now_ms - (args.hyperliquid_hours as i64) * 3_600 * 1_000;

    let mut all_rows: Vec<Rate> = Vec::new();
    let mut per_venue: BTreeMap<&'static str, usize> = BTreeMap::new();

    for sym in &args.symbols {
        let sym_upper = sym.to_uppercase();

        if !args.no_okx {
            let inst_id = format!("{sym_upper}-USDT-SWAP");
            match okx::fetch_funding(
                &client,
                &cfg,
                &inst_id,
                &sym_upper,
                args.okx_limit,
                None,
                None,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    tracing::info!(symbol = %sym_upper, venue = "okx", rows = rows.len(), "decoded");
                    *per_venue.entry("okx").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %sym_upper, venue = "okx", error = %e, "fetch failed; continuing"),
            }
            sleep_if(cfg.rate_limit_delay).await;
        }

        if !args.no_coinbase_intl {
            let exchange_symbol = format!("{sym_upper}-PERP");
            match coinbase_intl::fetch_funding(
                &client,
                &cfg,
                &exchange_symbol,
                &sym_upper,
                args.coinbase_limit,
                0,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    tracing::info!(symbol = %sym_upper, venue = "coinbase_intl", rows = rows.len(), "decoded");
                    *per_venue.entry("coinbase_intl").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %sym_upper, venue = "coinbase_intl", error = %e, "fetch failed; continuing"),
            }
            sleep_if(cfg.rate_limit_delay).await;
        }

        if !args.no_hyperliquid {
            match hyperliquid::fetch_funding(
                &client,
                &cfg,
                &sym_upper,
                hl_start_ms,
                None,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    tracing::info!(symbol = %sym_upper, venue = "hyperliquid", rows = rows.len(), "decoded");
                    *per_venue.entry("hyperliquid").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %sym_upper, venue = "hyperliquid", error = %e, "fetch failed; continuing"),
            }
            sleep_if(cfg.rate_limit_delay).await;
        }

        if !args.no_dydx_v4 {
            let ticker = format!("{sym_upper}-USD");
            match dydx_v4::fetch_funding(
                &client,
                &cfg,
                &ticker,
                &sym_upper,
                None,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    tracing::info!(symbol = %sym_upper, venue = "dydx_v4", rows = rows.len(), "decoded");
                    *per_venue.entry("dydx_v4").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %sym_upper, venue = "dydx_v4", error = %e, "fetch failed; continuing"),
            }
            sleep_if(cfg.rate_limit_delay).await;
        }
    }

    if all_rows.is_empty() {
        println!("cex-funding multi: rows_added=0 (no rows from any venue)");
        return Ok(());
    }

    // Bucket by canonical symbol for the partition-key write.
    let mut by_symbol: BTreeMap<String, Vec<Rate>> = BTreeMap::new();
    for r in all_rows {
        by_symbol.entry(r.symbol.clone()).or_default().push(r);
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (sym, rows) in &by_symbol {
        let stats = ds
            .write::<Rate>(&args.venue, Some(sym), rows)
            .with_context(|| format!("Dataset::write symbol={sym}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }

    println!(
        "cex-funding multi: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_venue={per_venue:?}"
    );
    Ok(())
}

async fn sleep_if(d: Duration) {
    if d > Duration::ZERO {
        tokio::time::sleep(d).await;
    }
}
