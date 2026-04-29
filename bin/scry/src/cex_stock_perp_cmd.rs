//! `scry cex-stock-perp tape` — multi-venue stock-perp tape poll.
//!
//! Single-tick poll across the configured venues for the configured
//! xStock underlier set. Schedule cadence externally via launchd /
//! cron (typical: 60s).
//!
//! v1 ships 4 venues (Kraken Futures, Gate.io, OKX, Coinbase
//! International); the remaining 7 from `wishlist.md` item 45 are
//! follow-up enrichment modules.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_cex_perps::{
    bingx, bitget, build_client, coinbase_intl, crypto_com, gate, htx, kraken_futures,
    kucoin_futures, mexc, okx, phemex, PollConfig,
};
use scryer_schema::cex_stock_perp_tape::v1::Tick;
use scryer_schema::{cex_stock_perp_tape, Meta};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct TapeArgs {
    /// Comma-separated canonical underlier symbols.
    #[arg(long, value_delimiter = ',', default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT")]
    underliers: Vec<String>,
    /// Disable Kraken Futures.
    #[arg(long, default_value_t = false)]
    no_kraken_futures: bool,
    /// Disable Gate.io.
    #[arg(long, default_value_t = false)]
    no_gate: bool,
    /// Disable OKX.
    #[arg(long, default_value_t = false)]
    no_okx: bool,
    /// Disable Coinbase International.
    #[arg(long, default_value_t = false)]
    no_coinbase_intl: bool,
    /// Disable Bitget.
    #[arg(long, default_value_t = false)]
    no_bitget: bool,
    /// Disable HTX.
    #[arg(long, default_value_t = false)]
    no_htx: bool,
    /// Disable BingX.
    #[arg(long, default_value_t = false)]
    no_bingx: bool,
    /// Disable MEXC.
    #[arg(long, default_value_t = false)]
    no_mexc: bool,
    /// Disable KuCoin Futures.
    #[arg(long, default_value_t = false)]
    no_kucoin_futures: bool,
    /// Disable Phemex.
    #[arg(long, default_value_t = false)]
    no_phemex: bool,
    /// Disable Crypto.com.
    #[arg(long, default_value_t = false)]
    no_crypto_com: bool,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-call delay within a venue's symbol loop.
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CEX_STOCK_PERP)]
    venue: String,
}

pub async fn run_tape(args: TapeArgs) -> Result<()> {
    if args.underliers.is_empty() {
        anyhow::bail!("--underliers cannot be empty");
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
    let underliers_upper: Vec<String> = args
        .underliers
        .iter()
        .map(|s| s.to_uppercase())
        .collect();

    let mut all_rows: Vec<Tick> = Vec::new();
    let mut per_venue: BTreeMap<&'static str, usize> = BTreeMap::new();

    if !args.no_kraken_futures {
        match kraken_futures::fetch_stock_perps(&client, &cfg, Some(&underliers_upper), fetched_at)
            .await
        {
            Ok(rows) => {
                tracing::info!(venue = "kraken_futures", rows = rows.len(), "decoded");
                *per_venue.entry("kraken_futures").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "kraken_futures", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_gate {
        match gate::fetch_stock_perps(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "gate", rows = rows.len(), "decoded");
                *per_venue.entry("gate").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "gate", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_okx {
        match okx::fetch_tape(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "okx", rows = rows.len(), "decoded");
                *per_venue.entry("okx").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "okx", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_coinbase_intl {
        match coinbase_intl::fetch_tape(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "coinbase_intl", rows = rows.len(), "decoded");
                *per_venue.entry("coinbase_intl").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "coinbase_intl", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_bitget {
        match bitget::fetch_stock_perps(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "bitget", rows = rows.len(), "decoded");
                *per_venue.entry("bitget").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "bitget", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_kucoin_futures {
        match kucoin_futures::fetch_stock_perps(&client, &cfg, &underliers_upper, fetched_at).await {
            Ok(rows) => {
                tracing::info!(venue = "kucoin_futures", rows = rows.len(), "decoded");
                *per_venue.entry("kucoin_futures").or_insert(0) += rows.len();
                all_rows.extend(rows);
            }
            Err(e) => tracing::warn!(venue = "kucoin_futures", error = %e, "fetch failed; continuing"),
        }
    }
    if !args.no_htx {
        let mut htx_rows = 0usize;
        for u in &underliers_upper {
            // Try X-suffix (xstock_backed) first, then plain (synthetic).
            for (sym, backing) in [
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("{u}-USDT"), "synthetic"),
            ] {
                match htx::fetch_one_ticker(&client, &cfg, &sym, u, backing, fetched_at).await {
                    Ok(Some(t)) => {
                        all_rows.push(t);
                        htx_rows += 1;
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
                if cfg.rate_limit_delay > Duration::ZERO {
                    tokio::time::sleep(cfg.rate_limit_delay).await;
                }
            }
        }
        tracing::info!(venue = "htx", rows = htx_rows, "decoded");
        *per_venue.entry("htx").or_insert(0) += htx_rows;
    }
    if !args.no_bingx {
        let mut bingx_rows = 0usize;
        for u in &underliers_upper {
            // Try X-suffix (xstock_backed) and NCSK-prefix (synthetic).
            for (sym, backing) in [
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("NCSK{u}2USD-USDT"), "synthetic"),
            ] {
                match bingx::fetch_one_ticker(&client, &cfg, &sym, u, backing, fetched_at).await {
                    Ok(Some(t)) => {
                        all_rows.push(t);
                        bingx_rows += 1;
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
                if cfg.rate_limit_delay > Duration::ZERO {
                    tokio::time::sleep(cfg.rate_limit_delay).await;
                }
            }
        }
        tracing::info!(venue = "bingx", rows = bingx_rows, "decoded");
        *per_venue.entry("bingx").or_insert(0) += bingx_rows;
    }
    if !args.no_mexc {
        let mut mexc_rows = 0usize;
        for u in &underliers_upper {
            let sym = format!("{u}STOCK_USDT");
            match mexc::fetch_one_ticker(&client, &cfg, &sym, u, fetched_at).await {
                Ok(Some(t)) => {
                    all_rows.push(t);
                    mexc_rows += 1;
                }
                Ok(None) => {}
                Err(_) => {}
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "mexc", rows = mexc_rows, "decoded");
        *per_venue.entry("mexc").or_insert(0) += mexc_rows;
    }
    if !args.no_phemex {
        let mut phemex_rows = 0usize;
        for u in &underliers_upper {
            // Try X-suffix (xstock_backed) and plain (synthetic).
            for (sym, backing) in [
                (format!("{u}XUSDT"), "xstock_backed"),
                (format!("{u}USDT"), "synthetic"),
            ] {
                match phemex::fetch_one_ticker(&client, &cfg, &sym, u, backing, fetched_at).await {
                    Ok(Some(t)) => {
                        all_rows.push(t);
                        phemex_rows += 1;
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
                if cfg.rate_limit_delay > Duration::ZERO {
                    tokio::time::sleep(cfg.rate_limit_delay).await;
                }
            }
        }
        tracing::info!(venue = "phemex", rows = phemex_rows, "decoded");
        *per_venue.entry("phemex").or_insert(0) += phemex_rows;
    }
    if !args.no_crypto_com {
        let mut cc_rows = 0usize;
        for u in &underliers_upper {
            let sym = format!("{u}USD-PERP");
            match crypto_com::fetch_one_ticker(&client, &cfg, &sym, u, fetched_at).await {
                Ok(Some(t)) => {
                    all_rows.push(t);
                    cc_rows += 1;
                }
                Ok(None) => {}
                Err(_) => {}
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "crypto_com", rows = cc_rows, "decoded");
        *per_venue.entry("crypto_com").or_insert(0) += cc_rows;
    }

    if all_rows.is_empty() {
        println!("cex-stock-perp tape: rows_added=0 (no rows from any venue)");
        return Ok(());
    }

    // Stamp every row's meta.fetched_at to the snapshot time and
    // partition-write by underlier_symbol.
    let _ = Meta::new(cex_stock_perp_tape::v1::SCHEMA_VERSION, fetched_at, "");
    let mut by_underlier: BTreeMap<String, Vec<Tick>> = BTreeMap::new();
    for r in all_rows {
        by_underlier.entry(r.underlier_symbol.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (under, rows) in &by_underlier {
        let stats = ds
            .write::<Tick>(&args.venue, Some(under), rows)
            .with_context(|| format!("Dataset::write underlier={under}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "cex-stock-perp tape: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_venue={per_venue:?}"
    );
    Ok(())
}

// ============================================================
// 1m OHLCV companion (item 45 §1.2 / Phase 56)
// ============================================================

use scryer_schema::cex_stock_perp_ohlcv::v1::Bar as OhlcvBar;
use scryer_schema::cex_stock_perp_ohlcv;

#[derive(Parser, Debug)]
pub struct OhlcvArgs {
    /// Comma-separated canonical underlier symbols.
    #[arg(long, value_delimiter = ',', default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT")]
    underliers: Vec<String>,
    /// Disable Kraken Futures.
    #[arg(long, default_value_t = false)]
    no_kraken_futures: bool,
    /// Disable Gate.io.
    #[arg(long, default_value_t = false)]
    no_gate: bool,
    /// Disable OKX.
    #[arg(long, default_value_t = false)]
    no_okx: bool,
    /// Disable Coinbase International.
    #[arg(long, default_value_t = false)]
    no_coinbase_intl: bool,
    /// Disable Bitget.
    #[arg(long, default_value_t = false)]
    no_bitget: bool,
    /// Disable HTX.
    #[arg(long, default_value_t = false)]
    no_htx: bool,
    /// Disable BingX.
    #[arg(long, default_value_t = false)]
    no_bingx: bool,
    /// Disable MEXC.
    #[arg(long, default_value_t = false)]
    no_mexc: bool,
    /// Disable KuCoin Futures.
    #[arg(long, default_value_t = false)]
    no_kucoin_futures: bool,
    /// Disable Crypto.com.
    #[arg(long, default_value_t = false)]
    no_crypto_com: bool,
    /// Lookback window in minutes for the per-call request.
    /// Defaults to 60 (last hour); cron at the same cadence to
    /// roll forward.
    #[arg(long, default_value_t = 60)]
    lookback_minutes: i64,
    /// Per-venue OKX bar limit (max 300).
    #[arg(long, default_value_t = 100)]
    okx_limit: u32,
    /// Per-venue Gate.io bar limit (max 2000).
    #[arg(long, default_value_t = 100)]
    gate_limit: u32,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CEX_STOCK_PERP)]
    venue: String,
}

pub async fn run_ohlcv(args: OhlcvArgs) -> Result<()> {
    if args.underliers.is_empty() {
        anyhow::bail!("--underliers cannot be empty");
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
    let from_unix = fetched_at - args.lookback_minutes * 60;
    let underliers_upper: Vec<String> = args
        .underliers
        .iter()
        .map(|s| s.to_uppercase())
        .collect();

    let mut all_rows: Vec<OhlcvBar> = Vec::new();
    let mut per_venue: BTreeMap<&'static str, usize> = BTreeMap::new();

    if !args.no_kraken_futures {
        for u in &underliers_upper {
            let exchange_symbol = format!("PF_{u}XUSD");
            match kraken_futures::fetch_ohlcv(
                &client,
                &cfg,
                &exchange_symbol,
                from_unix,
                fetched_at,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    *per_venue.entry("kraken_futures").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %exchange_symbol, error = %e, "kraken_futures ohlcv skipped"),
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "kraken_futures", rows = per_venue.get("kraken_futures").copied().unwrap_or(0), "decoded");
    }

    if !args.no_gate {
        for u in &underliers_upper {
            // Try both X-suffix (xstock_backed) and plain (synthetic).
            for contract in [format!("{u}X_USDT"), format!("{u}_USDT")] {
                match gate::fetch_ohlcv(
                    &client,
                    &cfg,
                    &contract,
                    &underliers_upper,
                    Some(args.gate_limit),
                    fetched_at,
                )
                .await
                {
                    Ok(rows) => {
                        if !rows.is_empty() {
                            *per_venue.entry("gate").or_insert(0) += rows.len();
                            all_rows.extend(rows);
                        }
                    }
                    Err(_) => {
                        // Listing-gap is the common case; silently
                        // try the next variant.
                    }
                }
                if cfg.rate_limit_delay > Duration::ZERO {
                    tokio::time::sleep(cfg.rate_limit_delay).await;
                }
            }
        }
        tracing::info!(venue = "gate", rows = per_venue.get("gate").copied().unwrap_or(0), "decoded");
    }

    if !args.no_okx {
        for u in &underliers_upper {
            let inst_id = format!("{u}-USDT-SWAP");
            match okx::fetch_ohlcv(&client, &cfg, &inst_id, u, args.okx_limit, fetched_at).await {
                Ok(rows) => {
                    *per_venue.entry("okx").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %inst_id, error = %e, "okx ohlcv skipped"),
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "okx", rows = per_venue.get("okx").copied().unwrap_or(0), "decoded");
    }

    if !args.no_coinbase_intl {
        let start_iso = format_unix_as_iso(from_unix);
        for u in &underliers_upper {
            let exchange_symbol = format!("{u}-PERP");
            match coinbase_intl::fetch_ohlcv(
                &client,
                &cfg,
                &exchange_symbol,
                u,
                &start_iso,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    *per_venue.entry("coinbase_intl").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(e) => tracing::warn!(symbol = %exchange_symbol, error = %e, "coinbase_intl ohlcv skipped"),
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "coinbase_intl", rows = per_venue.get("coinbase_intl").copied().unwrap_or(0), "decoded");
    }

    if !args.no_bitget {
        for u in &underliers_upper {
            let sym = format!("{u}USDT");
            match bitget::fetch_ohlcv(&client, &cfg, &sym, u, args.gate_limit, fetched_at).await {
                Ok(rows) => {
                    *per_venue.entry("bitget").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(_) => {}
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "bitget", rows = per_venue.get("bitget").copied().unwrap_or(0), "decoded");
    }

    if !args.no_kucoin_futures {
        for u in &underliers_upper {
            let sym = format!("{u}USDTM");
            match kucoin_futures::fetch_ohlcv(
                &client,
                &cfg,
                &sym,
                u,
                from_unix,
                fetched_at,
                fetched_at,
            )
            .await
            {
                Ok(rows) => {
                    *per_venue.entry("kucoin_futures").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(_) => {}
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "kucoin_futures", rows = per_venue.get("kucoin_futures").copied().unwrap_or(0), "decoded");
    }

    if !args.no_htx {
        for u in &underliers_upper {
            for (sym, backing) in [
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("{u}-USDT"), "synthetic"),
            ] {
                match htx::fetch_ohlcv(
                    &client,
                    &cfg,
                    &sym,
                    u,
                    backing,
                    args.gate_limit,
                    fetched_at,
                )
                .await
                {
                    Ok(rows) if !rows.is_empty() => {
                        *per_venue.entry("htx").or_insert(0) += rows.len();
                        all_rows.extend(rows);
                    }
                    _ => {}
                }
                if cfg.rate_limit_delay > Duration::ZERO {
                    tokio::time::sleep(cfg.rate_limit_delay).await;
                }
            }
        }
        tracing::info!(venue = "htx", rows = per_venue.get("htx").copied().unwrap_or(0), "decoded");
    }

    if !args.no_bingx {
        for u in &underliers_upper {
            for (sym, backing) in [
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("NCSK{u}2USD-USDT"), "synthetic"),
            ] {
                match bingx::fetch_ohlcv(
                    &client,
                    &cfg,
                    &sym,
                    u,
                    backing,
                    args.gate_limit,
                    fetched_at,
                )
                .await
                {
                    Ok(rows) if !rows.is_empty() => {
                        *per_venue.entry("bingx").or_insert(0) += rows.len();
                        all_rows.extend(rows);
                    }
                    _ => {}
                }
                if cfg.rate_limit_delay > Duration::ZERO {
                    tokio::time::sleep(cfg.rate_limit_delay).await;
                }
            }
        }
        tracing::info!(venue = "bingx", rows = per_venue.get("bingx").copied().unwrap_or(0), "decoded");
    }

    if !args.no_mexc {
        for u in &underliers_upper {
            let sym = format!("{u}STOCK_USDT");
            match mexc::fetch_ohlcv(&client, &cfg, &sym, u, from_unix, fetched_at).await {
                Ok(rows) => {
                    *per_venue.entry("mexc").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(_) => {}
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "mexc", rows = per_venue.get("mexc").copied().unwrap_or(0), "decoded");
    }

    if !args.no_crypto_com {
        for u in &underliers_upper {
            let sym = format!("{u}USD-PERP");
            match crypto_com::fetch_ohlcv(&client, &cfg, &sym, u, args.gate_limit, fetched_at).await {
                Ok(rows) => {
                    *per_venue.entry("crypto_com").or_insert(0) += rows.len();
                    all_rows.extend(rows);
                }
                Err(_) => {}
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(venue = "crypto_com", rows = per_venue.get("crypto_com").copied().unwrap_or(0), "decoded");
    }

    if all_rows.is_empty() {
        println!("cex-stock-perp ohlcv: rows_added=0 (no rows from any venue)");
        return Ok(());
    }

    let mut by_underlier: BTreeMap<String, Vec<OhlcvBar>> = BTreeMap::new();
    for r in all_rows {
        by_underlier.entry(r.underlier_symbol.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (under, rows) in &by_underlier {
        let stats = ds
            .write::<OhlcvBar>(&args.venue, Some(under), rows)
            .with_context(|| format!("Dataset::write underlier={under}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    let _ = cex_stock_perp_ohlcv::v1::SCHEMA_VERSION;
    println!(
        "cex-stock-perp ohlcv: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_venue={per_venue:?}"
    );
    Ok(())
}

fn format_unix_as_iso(unix: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(unix, 0)
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

// ============================================================
// Kraken Futures historical OHLCV backfill (item 45 / Phase 58)
// ============================================================

#[derive(Parser, Debug)]
pub struct BackfillArgs {
    /// Venue to backfill. Currently only `kraken_futures` exposes
    /// deep history per `PF_*XUSD` listing date. Other venues cap
    /// at ~30-90 days; if needed, a v2 follow-up adds per-venue
    /// backfill paths.
    #[arg(long, default_value = "kraken_futures")]
    venue: String,
    /// Comma-separated canonical underlier symbols.
    #[arg(long, value_delimiter = ',', default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD")]
    underliers: Vec<String>,
    /// Window start (`YYYY-MM-DD` UTC).
    #[arg(long)]
    start: String,
    /// Window end (`YYYY-MM-DD` UTC, inclusive). Default: today.
    #[arg(long, default_value = "")]
    end: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-call delay within the chunk loop (milliseconds).
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CEX_STOCK_PERP)]
    dataset_venue: String,
}

pub async fn run_backfill(args: BackfillArgs) -> Result<()> {
    if args.underliers.is_empty() {
        anyhow::bail!("--underliers cannot be empty");
    }
    if args.venue != "kraken_futures" {
        anyhow::bail!(
            "only --venue kraken_futures is supported in v1; other venues cap at ~30-90 days of forward-only candles"
        );
    }
    let start_ts = parse_ymd(&args.start)?;
    let end_ts = if args.end.is_empty() {
        Utc::now().timestamp()
    } else {
        parse_ymd(&args.end)? + 86_400
    };
    if end_ts <= start_ts {
        anyhow::bail!("--end must be after --start");
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
    let underliers_upper: Vec<String> =
        args.underliers.iter().map(|s| s.to_uppercase()).collect();

    let mut all_rows: Vec<OhlcvBar> = Vec::new();
    let mut per_underlier: BTreeMap<String, usize> = BTreeMap::new();
    for u in &underliers_upper {
        let exchange_symbol = format!("PF_{u}XUSD");
        let mut cursor = start_ts;
        let mut underlier_rows = 0usize;
        loop {
            // Kraken caps at 2000 bars/call (~1.39 days at 1m). Walk
            // the window forward until we exhaust it or upstream
            // returns nothing.
            match kraken_futures::fetch_ohlcv(
                &client,
                &cfg,
                &exchange_symbol,
                cursor,
                end_ts,
                fetched_at,
            )
            .await
            {
                Ok(rows) if !rows.is_empty() => {
                    let last_ts = rows.last().unwrap().bar_open_ts;
                    underlier_rows += rows.len();
                    all_rows.extend(rows);
                    let next = last_ts + 60;
                    if next <= cursor || next > end_ts {
                        break;
                    }
                    cursor = next;
                }
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!(symbol = %exchange_symbol, cursor, error = %e, "kraken backfill chunk failed; advancing");
                    cursor += 86_400; // skip a day on error
                    if cursor >= end_ts {
                        break;
                    }
                }
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(symbol = %exchange_symbol, rows = underlier_rows, "backfill complete");
        per_underlier.insert(u.clone(), underlier_rows);
    }

    if all_rows.is_empty() {
        println!("cex-stock-perp backfill: rows_added=0 (no rows from kraken_futures)");
        return Ok(());
    }

    let mut by_underlier: BTreeMap<String, Vec<OhlcvBar>> = BTreeMap::new();
    for r in all_rows {
        by_underlier.entry(r.underlier_symbol.clone()).or_default().push(r);
    }
    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (under, rows) in &by_underlier {
        let stats = ds
            .write::<OhlcvBar>(&args.dataset_venue, Some(under), rows)
            .with_context(|| format!("Dataset::write underlier={under}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "cex-stock-perp backfill: rows_added={total_added} rows_deduped={total_deduped} partitions_written={total_partitions} per_underlier_rows={per_underlier:?}"
    );
    Ok(())
}

fn parse_ymd(s: &str) -> Result<i64> {
    use chrono::TimeZone;
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("expected YYYY-MM-DD, got {s}"))?;
    let naive = d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?;
    Ok(chrono::Utc.from_utc_datetime(&naive).timestamp())
}
