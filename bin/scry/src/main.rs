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
        },
        Command::Solana(c) => match c.target {
            SolanaTarget::Swaps(a) => solana_cmd::run_swaps(a).await,
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
