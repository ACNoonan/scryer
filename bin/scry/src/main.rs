//! `scry` — scryer CLI.
//!
//! Subcommands:
//!
//!   scry import swaps  --input PATH --venue VENUE --pool POOL [--source LABEL] [--dataset DIR]
//!   scry import trades --input PATH --venue VENUE --pair PAIR [--source LABEL] [--dataset DIR]
//!   scry solana swaps  --pool-metadata FILE --start DATE --end DATE
//!                      --proxy-url URL --helius-api-key KEY
//!                      [--dataset DIR] [--venue VENUE]

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand};

mod import_cmd;
mod solana_cmd;

#[derive(Parser, Debug)]
#[command(name = "scry", version, about = "scryer CLI: fetch + import + manage")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Import existing parquet into scryer's `dataset/` layout.
    Import(ImportCmd),
    /// Solana fetchers — Raydium swaps via proxy + Helius parseTransactions.
    Solana(SolanaCmd),
}

#[derive(Parser, Debug)]
struct ImportCmd {
    #[command(subcommand)]
    target: ImportTarget,
}

#[derive(Subcommand, Debug)]
enum ImportTarget {
    /// Import a swap.v1 parquet (e.g. quant-work/data/raydium_solusdc_swaps.parquet).
    Swaps(import_cmd::SwapsArgs),
    /// Import a trade.v1 parquet (e.g. quant-work/data/kraken_solusd_trades.parquet).
    Trades(import_cmd::TradesArgs),
    /// Import a kamino_scope.v1 parquet (e.g. soothsayer/data/raw/kamino_scope_tape_*.parquet).
    KaminoScope(import_cmd::KaminoScopeArgs),
    /// Import a pyth.v1 parquet (e.g. soothsayer/data/raw/pyth_xstock_tape_*.parquet).
    Pyth(import_cmd::PythArgs),
    /// Import a v5_tape.v1 parquet (e.g. soothsayer/data/raw/v5_tape_*.parquet).
    V5Tape(import_cmd::V5TapeArgs),
    /// Import a redstone.v1 parquet (e.g. soothsayer/data/processed/redstone_live_tape.parquet).
    Redstone(import_cmd::RedstoneArgs),
    /// Import yahoo.v1 OHLCV parquet(s) (e.g. soothsayer/data/raw/yahoo_*.parquet).
    /// Accepts multiple --input paths; merges them with dedup by (symbol, ts).
    Yahoo(import_cmd::YahooArgs),
    /// Import earnings.v1 calendar parquet(s) (e.g. soothsayer/data/raw/earnings_*.parquet).
    Earnings(import_cmd::EarningsArgs),
    /// Import backed.v1 corp-actions parquet (soothsayer/data/processed/backed_corp_actions.parquet).
    Backed(import_cmd::BackedArgs),
    /// Import nasdaq_halts.v1 RSS-halt parquet (soothsayer/data/processed/nasdaq_halts_live.parquet).
    NasdaqHalts(import_cmd::NasdaqHaltsArgs),
    /// Import kraken_funding.v1 funding-rate parquet(s) (soothsayer/data/raw/kraken_funding_*.parquet).
    KrakenFunding(import_cmd::KrakenFundingArgs),
}

#[derive(Parser, Debug)]
struct SolanaCmd {
    #[command(subcommand)]
    target: SolanaTarget,
}

#[derive(Subcommand, Debug)]
enum SolanaTarget {
    /// Fetch Raydium-v4 swaps from a window and write through scryer-store.
    Swaps(solana_cmd::SwapsArgs),
    /// Fetch Kamino Klend liquidation events from a window.
    KaminoLiquidations(solana_cmd::KaminoLiquidationsArgs),
    /// Fetch Jupiter Lend (Fluid Vaults) liquidation events from a window.
    JupiterLendLiquidations(solana_cmd::JupiterLendLiquidationsArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SCRY_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Import(c) => match c.target {
            ImportTarget::Swaps(a) => import_cmd::run_swaps(a).await,
            ImportTarget::Trades(a) => import_cmd::run_trades(a).await,
            ImportTarget::KaminoScope(a) => import_cmd::run_kamino_scope(a).await,
            ImportTarget::Pyth(a) => import_cmd::run_pyth(a).await,
            ImportTarget::V5Tape(a) => import_cmd::run_v5_tape(a).await,
            ImportTarget::Redstone(a) => import_cmd::run_redstone(a).await,
            ImportTarget::Yahoo(a) => import_cmd::run_yahoo(a).await,
            ImportTarget::Earnings(a) => import_cmd::run_earnings(a).await,
            ImportTarget::Backed(a) => import_cmd::run_backed(a).await,
            ImportTarget::NasdaqHalts(a) => import_cmd::run_nasdaq_halts(a).await,
            ImportTarget::KrakenFunding(a) => import_cmd::run_kraken_funding(a).await,
        },
        Command::Solana(c) => match c.target {
            SolanaTarget::Swaps(a) => solana_cmd::run_swaps(a).await,
            SolanaTarget::KaminoLiquidations(a) => solana_cmd::run_kamino_liquidations(a).await,
            SolanaTarget::JupiterLendLiquidations(a) => solana_cmd::run_jupiter_lend_liquidations(a).await,
        },
    }
}

/// Parse `YYYY-MM-DD` (UTC midnight) or full RFC 3339 into a unix
/// seconds timestamp. The first form is what scripts already use; the
/// second is for second-precision windows in tests.
pub fn parse_unix_seconds(s: &str) -> Result<i64> {
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = Utc
            .from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
        return Ok(dt.timestamp());
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }
    if let Ok(n) = s.parse::<i64>() {
        return Ok(n);
    }
    anyhow::bail!("could not parse `{s}` as YYYY-MM-DD, RFC 3339, or unix seconds")
}

pub fn cwd_dataset() -> PathBuf {
    PathBuf::from("./dataset")
}
