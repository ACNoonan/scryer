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
    kucoin_futures, mexc, okx, phemex, ws_bbo, PollConfig,
};
use scryer_schema::cex_stock_perp_bbo::v2::Row as BboRow;
use scryer_schema::cex_stock_perp_tape::v1::Tick;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct WsBboArgs {
    /// Comma-separated canonical underlier symbols.
    #[arg(long, value_delimiter = ',', default_value = "SPY,QQQ")]
    underliers: Vec<String>,
    /// Wall-clock capture duration. All enabled venue streams run concurrently.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,
    #[arg(long, default_value_t = false)]
    no_kraken_futures: bool,
    #[arg(long, default_value_t = false)]
    no_bitget: bool,
    #[arg(long, default_value_t = false)]
    no_okx: bool,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::CEX_AGGREGATE)]
    venue: String,
}

pub async fn run_ws_bbo(args: WsBboArgs) -> Result<()> {
    if args.underliers.is_empty() {
        anyhow::bail!("--underliers cannot be empty");
    }
    if args.duration_secs == 0 {
        anyhow::bail!("--duration-secs must be positive");
    }
    let started_us = Utc::now().timestamp_micros();
    let session_id = format!("{started_us}-{:010}", std::process::id());
    let result = ws_bbo::capture(
        &args.underliers,
        Duration::from_secs(args.duration_secs),
        &session_id,
        !args.no_kraken_futures,
        !args.no_bitget,
        !args.no_okx,
    )
    .await;
    for error in &result.errors {
        tracing::warn!(error, "CEX stock-perp WebSocket capture venue failed");
    }

    let mut by_underlier: BTreeMap<String, Vec<BboRow>> = BTreeMap::new();
    let mut per_venue: BTreeMap<String, usize> = BTreeMap::new();
    for row in result.rows {
        *per_venue.entry(row.exchange.clone()).or_default() += 1;
        by_underlier
            .entry(row.underlier_symbol.clone())
            .or_default()
            .push(row);
    }
    let ds = Dataset::new(&args.dataset);
    let mut rows_added = 0;
    let mut rows_deduped = 0;
    let mut partitions_written = 0;
    for (underlier, rows) in &by_underlier {
        let stats = ds
            .write::<BboRow>(&args.venue, Some(underlier), rows)
            .with_context(|| format!("Dataset::write underlier={underlier}"))?;
        rows_added += stats.rows_added;
        rows_deduped += stats.rows_deduped;
        partitions_written += stats.partitions_written;
    }
    println!(
        "cex-stock-perp ws-bbo: session_id={session_id} rows_added={rows_added} \
         rows_deduped={rows_deduped} partitions_written={partitions_written} \
         per_venue={per_venue:?} errors={:?}",
        result.errors
    );
    Ok(())
}

#[derive(Parser, Debug)]
pub struct TapeArgs {
    /// Comma-separated canonical underlier symbols.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT"
    )]
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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
    let capture_started_at_us = now.timestamp_micros();
    let capture_id = format!("{capture_started_at_us}-{:010}", std::process::id());
    let underliers_upper: Vec<String> = args.underliers.iter().map(|s| s.to_uppercase()).collect();

    let enabled = [
        (!args.no_kraken_futures, TapeVenue::KrakenFutures),
        (!args.no_gate, TapeVenue::Gate),
        (!args.no_okx, TapeVenue::Okx),
        (!args.no_coinbase_intl, TapeVenue::CoinbaseIntl),
        (!args.no_bitget, TapeVenue::Bitget),
        (!args.no_kucoin_futures, TapeVenue::KucoinFutures),
        (!args.no_htx, TapeVenue::Htx),
        (!args.no_bingx, TapeVenue::Bingx),
        (!args.no_mexc, TapeVenue::Mexc),
        (!args.no_phemex, TapeVenue::Phemex),
        (!args.no_crypto_com, TapeVenue::CryptoCom),
    ];
    let mut tasks = tokio::task::JoinSet::new();
    for (_, venue) in enabled.into_iter().filter(|(on, _)| *on) {
        let task_client = client.clone();
        let task_cfg = cfg.clone();
        let task_underliers = underliers_upper.clone();
        tasks.spawn(async move {
            let rows =
                poll_tape_venue(venue, &task_client, &task_cfg, &task_underliers, fetched_at).await;
            (venue, rows, Utc::now().timestamp_micros())
        });
    }

    let mut all_rows: Vec<Tick> = Vec::new();
    let mut per_venue: BTreeMap<&'static str, usize> = BTreeMap::new();
    while let Some(joined) = tasks.join_next().await {
        let (venue, mut rows, available_at_us) =
            joined.context("CEX stock-perp venue task panicked")?;
        let available_at = available_at_us.div_euclid(1_000_000);
        for row in &mut rows {
            row.ts = available_at;
            row.meta.fetched_at = available_at;
            row.capture_id = Some(capture_id.clone());
            row.capture_started_at_us = Some(capture_started_at_us);
            row.available_at_us = Some(available_at_us);
        }
        tracing::info!(
            venue = venue.name(),
            rows = rows.len(),
            available_at_us,
            "decoded"
        );
        per_venue.insert(venue.name(), rows.len());
        all_rows.extend(rows);
    }

    if all_rows.is_empty() {
        println!("cex-stock-perp tape: rows_added=0 (no rows from any venue)");
        return Ok(());
    }

    // Rows already carry their per-venue local-availability time.
    // Partition-write by underlier_symbol.
    let mut by_underlier: BTreeMap<String, Vec<Tick>> = BTreeMap::new();
    for r in all_rows {
        by_underlier
            .entry(r.underlier_symbol.clone())
            .or_default()
            .push(r);
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

#[derive(Clone, Copy)]
enum TapeVenue {
    KrakenFutures,
    Gate,
    Okx,
    CoinbaseIntl,
    Bitget,
    KucoinFutures,
    Htx,
    Bingx,
    Mexc,
    Phemex,
    CryptoCom,
}

impl TapeVenue {
    fn name(self) -> &'static str {
        match self {
            Self::KrakenFutures => "kraken_futures",
            Self::Gate => "gate",
            Self::Okx => "okx",
            Self::CoinbaseIntl => "coinbase_intl",
            Self::Bitget => "bitget",
            Self::KucoinFutures => "kucoin_futures",
            Self::Htx => "htx",
            Self::Bingx => "bingx",
            Self::Mexc => "mexc",
            Self::Phemex => "phemex",
            Self::CryptoCom => "crypto_com",
        }
    }
}

async fn poll_tape_venue(
    venue: TapeVenue,
    client: &reqwest::Client,
    cfg: &PollConfig,
    underliers: &[String],
    fetched_at: i64,
) -> Vec<Tick> {
    let result = match venue {
        TapeVenue::KrakenFutures => {
            kraken_futures::fetch_stock_perps(client, cfg, Some(underliers), fetched_at).await
        }
        TapeVenue::Gate => gate::fetch_stock_perps(client, cfg, underliers, fetched_at).await,
        TapeVenue::Okx => okx::fetch_tape(client, cfg, underliers, fetched_at).await,
        TapeVenue::CoinbaseIntl => {
            coinbase_intl::fetch_tape(client, cfg, underliers, fetched_at).await
        }
        TapeVenue::Bitget => bitget::fetch_stock_perps(client, cfg, underliers, fetched_at).await,
        TapeVenue::KucoinFutures => {
            kucoin_futures::fetch_stock_perps(client, cfg, underliers, fetched_at).await
        }
        TapeVenue::Htx
        | TapeVenue::Bingx
        | TapeVenue::Mexc
        | TapeVenue::Phemex
        | TapeVenue::CryptoCom => {
            return poll_symbol_loop(venue, client, cfg, underliers, fetched_at).await
        }
    };
    match result {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(venue = venue.name(), error = %e, "fetch failed; continuing");
            Vec::new()
        }
    }
}

async fn poll_symbol_loop(
    venue: TapeVenue,
    client: &reqwest::Client,
    cfg: &PollConfig,
    underliers: &[String],
    fetched_at: i64,
) -> Vec<Tick> {
    let mut rows = Vec::new();
    for u in underliers {
        let symbols: Vec<(String, &'static str)> = match venue {
            TapeVenue::Htx => vec![
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("{u}-USDT"), "synthetic"),
            ],
            TapeVenue::Bingx => vec![
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("NCSK{u}2USD-USDT"), "synthetic"),
            ],
            TapeVenue::Mexc => vec![(format!("{u}STOCK_USDT"), "synthetic")],
            TapeVenue::Phemex => vec![
                (format!("{u}XUSDT"), "xstock_backed"),
                (format!("{u}USDT"), "synthetic"),
            ],
            TapeVenue::CryptoCom => vec![(format!("{u}USD-PERP"), "synthetic")],
            _ => unreachable!("aggregate venue sent to symbol loop"),
        };
        for (symbol, backing) in symbols {
            let row = match venue {
                TapeVenue::Htx => {
                    htx::fetch_one_ticker(client, cfg, &symbol, u, backing, fetched_at).await
                }
                TapeVenue::Bingx => {
                    bingx::fetch_one_ticker(client, cfg, &symbol, u, backing, fetched_at).await
                }
                TapeVenue::Mexc => {
                    mexc::fetch_one_ticker(client, cfg, &symbol, u, fetched_at).await
                }
                TapeVenue::Phemex => {
                    phemex::fetch_one_ticker(client, cfg, &symbol, u, backing, fetched_at).await
                }
                TapeVenue::CryptoCom => {
                    crypto_com::fetch_one_ticker(client, cfg, &symbol, u, fetched_at).await
                }
                _ => unreachable!("aggregate venue sent to symbol loop"),
            };
            match row {
                Ok(Some(tick)) => rows.push(tick),
                Ok(None) => {}
                Err(e) => tracing::debug!(
                    venue = venue.name(),
                    exchange_symbol = symbol,
                    error = %e,
                    "symbol unavailable"
                ),
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
    }
    rows
}

// ============================================================
// 1m OHLCV companion (item 45 §1.2 / Phase 56)
// ============================================================

use scryer_schema::cex_stock_perp_ohlcv;
use scryer_schema::cex_stock_perp_ohlcv::v1::Bar as OhlcvBar;

#[derive(Parser, Debug)]
pub struct OhlcvArgs {
    /// Comma-separated canonical underlier symbols.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT"
    )]
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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
    let underliers_upper: Vec<String> = args.underliers.iter().map(|s| s.to_uppercase()).collect();

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
                Err(e) => {
                    tracing::warn!(symbol = %exchange_symbol, error = %e, "kraken_futures ohlcv skipped")
                }
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(
            venue = "kraken_futures",
            rows = per_venue.get("kraken_futures").copied().unwrap_or(0),
            "decoded"
        );
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
        tracing::info!(
            venue = "gate",
            rows = per_venue.get("gate").copied().unwrap_or(0),
            "decoded"
        );
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
        tracing::info!(
            venue = "okx",
            rows = per_venue.get("okx").copied().unwrap_or(0),
            "decoded"
        );
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
                Err(e) => {
                    tracing::warn!(symbol = %exchange_symbol, error = %e, "coinbase_intl ohlcv skipped")
                }
            }
            if cfg.rate_limit_delay > Duration::ZERO {
                tokio::time::sleep(cfg.rate_limit_delay).await;
            }
        }
        tracing::info!(
            venue = "coinbase_intl",
            rows = per_venue.get("coinbase_intl").copied().unwrap_or(0),
            "decoded"
        );
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
        tracing::info!(
            venue = "bitget",
            rows = per_venue.get("bitget").copied().unwrap_or(0),
            "decoded"
        );
    }

    if !args.no_kucoin_futures {
        for u in &underliers_upper {
            let sym = format!("{u}USDTM");
            match kucoin_futures::fetch_ohlcv(
                &client, &cfg, &sym, u, from_unix, fetched_at, fetched_at,
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
        tracing::info!(
            venue = "kucoin_futures",
            rows = per_venue.get("kucoin_futures").copied().unwrap_or(0),
            "decoded"
        );
    }

    if !args.no_htx {
        for u in &underliers_upper {
            for (sym, backing) in [
                (format!("{u}X-USDT"), "xstock_backed"),
                (format!("{u}-USDT"), "synthetic"),
            ] {
                match htx::fetch_ohlcv(&client, &cfg, &sym, u, backing, args.gate_limit, fetched_at)
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
        tracing::info!(
            venue = "htx",
            rows = per_venue.get("htx").copied().unwrap_or(0),
            "decoded"
        );
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
        tracing::info!(
            venue = "bingx",
            rows = per_venue.get("bingx").copied().unwrap_or(0),
            "decoded"
        );
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
        tracing::info!(
            venue = "mexc",
            rows = per_venue.get("mexc").copied().unwrap_or(0),
            "decoded"
        );
    }

    if !args.no_crypto_com {
        for u in &underliers_upper {
            let sym = format!("{u}USD-PERP");
            match crypto_com::fetch_ohlcv(&client, &cfg, &sym, u, args.gate_limit, fetched_at).await
            {
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
        tracing::info!(
            venue = "crypto_com",
            rows = per_venue.get("crypto_com").copied().unwrap_or(0),
            "decoded"
        );
    }

    if all_rows.is_empty() {
        println!("cex-stock-perp ohlcv: rows_added=0 (no rows from any venue)");
        return Ok(());
    }

    let mut by_underlier: BTreeMap<String, Vec<OhlcvBar>> = BTreeMap::new();
    for r in all_rows {
        by_underlier
            .entry(r.underlier_symbol.clone())
            .or_default()
            .push(r);
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
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD"
    )]
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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
    let underliers_upper: Vec<String> = args.underliers.iter().map(|s| s.to_uppercase()).collect();

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
        by_underlier
            .entry(r.underlier_symbol.clone())
            .or_default()
            .push(r);
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
