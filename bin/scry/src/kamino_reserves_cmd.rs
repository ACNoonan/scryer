use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::jupiter::XSTOCK_MINTS;
use scryer_fetch_solana::kamino_reserves::{
    fetch_reserves_for_xstocks, KaminoReservesFetcherConfig, XstockMint,
};
use scryer_schema::{kamino_reserve, Meta};
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct ReservesArgs {
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Optional comma-separated list of symbols to scan. Defaults to
    /// the canonical 8-xStock registry from `scryer-fetch-dexagg`.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getProgramAccounts")]
    source: String,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    /// Venue under which reserves are written. Defaults to `kamino` so
    /// snapshots co-locate with Phase-17 kamino_liquidations data.
    #[arg(long, default_value = scryer_store::venue::KAMINO)]
    venue: String,
}

pub async fn run_reserves(args: ReservesArgs) -> Result<()> {
    let targets: Vec<XstockMint> = if args.symbols.is_empty() {
        XSTOCK_MINTS
            .iter()
            .map(|(s, m)| XstockMint {
                symbol: s.to_string(),
                mint: m.to_string(),
            })
            .collect()
    } else {
        let want: std::collections::HashSet<&str> =
            args.symbols.iter().map(|s| s.as_str()).collect();
        XSTOCK_MINTS
            .iter()
            .filter(|(s, _)| want.contains(s))
            .map(|(s, m)| XstockMint {
                symbol: s.to_string(),
                mint: m.to_string(),
            })
            .collect()
    };
    if targets.is_empty() {
        anyhow::bail!("no symbols matched; use one of: {:?}", XSTOCK_MINTS.iter().map(|(s, _)| *s).collect::<Vec<_>>());
    }

    let cfg = KaminoReservesFetcherConfig::new(args.proxy_url.clone());
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;
    let now = Utc::now();
    let meta = Meta::new(
        kamino_reserve::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    tracing::info!(
        symbols = targets.len(),
        proxy = args.proxy_url,
        "scanning Kamino Klend reserves"
    );
    let reserves = fetch_reserves_for_xstocks(&client, &cfg, &targets, &meta)
        .await
        .context("fetch_reserves_for_xstocks")?;
    tracing::info!(
        reserves = reserves.len(),
        markets = reserves
            .iter()
            .map(|r| r.lending_market.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        "decoded; writing"
    );

    if reserves.is_empty() {
        println!("kamino_reserves snapshotted: 0 reserves matched");
        return Ok(());
    }
    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<kamino_reserve::v1::Reserve>(&args.venue, None, &reserves)
        .context("Dataset::write")?;
    println!(
        "kamino_reserves snapshotted: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
