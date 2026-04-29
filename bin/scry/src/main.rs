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

mod dexagg_cmd;
mod import_cmd;
mod jito_cmd;
mod jito_tip_floor_cmd;
mod kamino_obligations_cmd;
mod kamino_reserves_cmd;
mod cex_funding_cmd;
mod databento_cmd;
mod deribit_cmd;
mod dex_xstock_swaps_cmd;
mod drift_liquidations_cmd;
mod equities_cmd;
mod fred_cmd;
mod loopscale_loans_cmd;
mod mango_v4_liquidations_cmd;
mod mango_v4_oracle_configs_cmd;
mod oracle_context_cmd;
mod pyth_publisher_cmd;
mod rss_cmd;
mod pool_snapshots_cmd;
mod priority_fees_cmd;
mod pyth_cmd;
mod redstone_cmd;
mod solana_cmd;
mod v5_cmd;

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
    /// RedStone Live oracle-tape fetchers (REST, no proxy).
    Redstone(RedstoneCmd),
    /// Pyth Hermes oracle-tape fetchers (REST, no proxy).
    Pyth(PythCmd),
    /// DEX aggregator clients (GeckoTerminal). REST per-venue; no proxy.
    Dexagg(DexaggCmd),
    /// Soothsayer V5 tape — joined Chainlink + Jupiter observation
    /// per xStock per poll iteration. Soothsayer-experiment scope.
    V5tape(V5tapeCmd),
    /// Equity market-data fetchers (REST, no proxy). Stooq for OHLCV
    /// daily bars, Finnhub for the earnings calendar. Pivoted from
    /// Yahoo Finance to escape its bot-detection treadmill; replaces
    /// soothsayer's `run_v1_scrape.py`.
    Equities(EquitiesCmd),
    /// RSS / Atom-feed fetchers. `backed` polls the Backed Finance
    /// corp-actions GitHub commit feed; `nasdaq-halts` polls Nasdaq
    /// Trader's trade-halts RSS. Single-tick; cadence wrapped by
    /// launchd / cron.
    Rss(RssCmd),
    /// FRED macro release calendar (CPI / NFP / GDP / PCE / PPI /
    /// RetailSales by default). REST against api.stlouisfed.org;
    /// requires a free FRED_API_KEY.
    Fred(FredCmd),
    /// Databento Historical API — CME futures 1-minute OHLCV bars.
    /// Pay-as-you-go against the operator's $125 signup credit.
    Databento(DatabentoCmd),
    /// Deribit DVOL — BTC/ETH volatility-index fetcher (the
    /// crypto equivalent of CBOE's VIX). Public REST, no auth.
    /// Writes to dataset/deribit/dvol/v1/underlying={X}/year=YYYY.parquet.
    Deribit(DeribitCmd),
    /// Multi-venue perp-futures funding-rate fetcher (OKX, Coinbase
    /// International, Hyperliquid, dYdX v4). Public REST per venue;
    /// no auth, no proxy. Writes one row per (exchange, symbol,
    /// funding_ts) triple to dataset/cex_perp_funding/funding/v1/
    /// symbol={SYM}/year=Y/month=M/day=D.parquet.
    CexFunding(CexFundingCmd),
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

#[derive(Parser, Debug)]
struct RedstoneCmd {
    #[command(subcommand)]
    target: RedstoneTarget,
}

#[derive(Parser, Debug)]
struct PythCmd {
    #[command(subcommand)]
    target: PythTarget,
}

#[derive(Parser, Debug)]
struct DexaggCmd {
    #[command(subcommand)]
    target: DexaggTarget,
}

#[derive(Parser, Debug)]
struct V5tapeCmd {
    #[command(subcommand)]
    target: V5tapeTarget,
}

#[derive(Parser, Debug)]
struct EquitiesCmd {
    #[command(subcommand)]
    target: EquitiesTarget,
}

#[derive(Parser, Debug)]
struct RssCmd {
    #[command(subcommand)]
    target: RssTarget,
}

#[derive(Parser, Debug)]
struct FredCmd {
    #[command(subcommand)]
    target: FredTarget,
}

#[derive(Parser, Debug)]
struct DatabentoCmd {
    #[command(subcommand)]
    target: DatabentoTarget,
}

#[derive(Parser, Debug)]
struct DeribitCmd {
    #[command(subcommand)]
    target: DeribitTarget,
}

#[derive(Subcommand, Debug)]
enum DeribitTarget {
    /// Pull DVOL closes for the configured currencies over the
    /// lookback window. Writes one
    /// `deribit_iv.v1::DvolBar` row per (currency, ts) pair.
    Dvol(deribit_cmd::DvolArgs),
}

#[derive(Parser, Debug)]
struct CexFundingCmd {
    #[command(subcommand)]
    target: CexFundingTarget,
}

#[derive(Subcommand, Debug)]
enum CexFundingTarget {
    /// Poll OKX + Coinbase International + Hyperliquid + dYdX v4 for
    /// the configured symbols. Each venue can be disabled via
    /// `--no-{venue}`. Writes to dataset/cex_perp_funding/funding/v1/
    /// symbol={SYM}/year=Y/month=M/day=D.parquet.
    Multi(cex_funding_cmd::MultiArgs),
}

#[derive(Subcommand, Debug)]
enum DatabentoTarget {
    /// CME futures 1-minute OHLCV bars (GLBX.MDP3 dataset, volume-
    /// rolled continuous front-month contracts ES.v.0/NQ.v.0/GC.v.0/
    /// ZN.v.0). Writes
    /// dataset/cme/intraday_1m/v1/symbol={X}/year=Y/month=M/day=D.parquet.
    Intraday1m(databento_cmd::IntradayArgs),
    /// Daily OHLCV equity bars via DBEQ.BASIC (Databento's
    /// consolidated US-equity dataset). Writes to a separate venue
    /// (`databento`, not `yahoo`) so cross-source validation against
    /// Stooq-sourced bars is possible without parquet collisions.
    EquitiesDaily(databento_cmd::EquitiesDailyArgs),
}

#[derive(Subcommand, Debug)]
enum FredTarget {
    /// Pull scheduled + historical release dates for the configured
    /// FRED release set in `[start, end]`. Writes
    /// dataset/fred/macro_calendar/v1/year=YYYY.parquet.
    MacroCalendar(fred_cmd::MacroCalendarArgs),
    /// Pull daily-resolution observations for one or more FRED
    /// series IDs (TIPS breakevens, credit spreads, treasury
    /// yields, term-premium proxies). Writes
    /// dataset/fred/macro_extended/v1/series={SID}/year=YYYY.parquet.
    Series(fred_cmd::SeriesArgs),
}

#[derive(Subcommand, Debug)]
enum RssTarget {
    /// Backed Finance corp-actions GitHub commit feed.
    Backed(rss_cmd::BackedArgs),
    /// Nasdaq Trader trade-halts RSS feed.
    NasdaqHalts(rss_cmd::NasdaqHaltsArgs),
}

#[derive(Subcommand, Debug)]
enum EquitiesTarget {
    /// Daily OHLCV bars from Stooq across the requested symbols +
    /// window. Writes one yearly parquet per symbol under
    /// dataset/yahoo/equities_daily/v1/ (the venue path retains the
    /// historical `yahoo` name for parquet-layout backward compat).
    Bars(equities_cmd::BarsArgs),
    /// Earnings dates from Finnhub per symbol. Writes one yearly
    /// parquet per symbol under dataset/yahoo/earnings/v1/.
    Earnings(equities_cmd::EarningsArgs),
}

#[derive(Subcommand, Debug)]
enum RedstoneTarget {
    /// One-tick poll of api.redstone.finance/prices for the
    /// configured symbols. Schedule via launchd / cron at the
    /// desired cadence (typical: 10m).
    Tape(redstone_cmd::TapeArgs),
}

#[derive(Subcommand, Debug)]
enum PythTarget {
    /// One-tick poll of hermes.pyth.network/v2/updates/price/latest
    /// across all 32 default xStock feeds (8 symbols × 4 sessions).
    /// Schedule via launchd / cron at the desired cadence
    /// (typical: 60s).
    Tape(pyth_cmd::TapeArgs),
}

#[derive(Subcommand, Debug)]
enum DexaggTarget {
    /// GeckoTerminal pool-trades poll (free-tier returns latest ~300
    /// trades). Schedule via launchd / cron (typical: 15m). Idempotent
    /// — re-runs within the trade-coverage window dedup cleanly on
    /// `tx_hash`.
    GtTrades(dexagg_cmd::GtTradesArgs),
}

#[derive(Subcommand, Debug)]
enum V5tapeTarget {
    /// One-tick joined Chainlink + Jupiter poll across the 8 xStocks.
    /// Schedule via launchd / cron at the desired cadence
    /// (typical: 60s).
    Tape(v5_cmd::TapeArgs),
}

#[derive(Subcommand, Debug)]
enum SolanaTarget {
    /// Fetch Raydium-v4 swaps from a window and write through scryer-store.
    Swaps(solana_cmd::SwapsArgs),
    /// Fetch Kamino Klend liquidation events from a window.
    KaminoLiquidations(solana_cmd::KaminoLiquidationsArgs),
    /// Fetch Jupiter Lend (Fluid Vaults) liquidation events from a window.
    JupiterLendLiquidations(solana_cmd::JupiterLendLiquidationsArgs),
    /// Snapshot Fluid Vaults VaultConfig accounts (one-shot, getProgramAccounts).
    FluidVaultConfigs(solana_cmd::FluidVaultConfigsArgs),
    /// One-tick Kamino-Scope tape collector (single getAccountInfo,
    /// 8 xStock symbols sliced locally). Schedule via launchd / cron
    /// at 60s cadence.
    KaminoScopeTape(solana_cmd::KaminoScopeTapeArgs),
    /// Hourly pool-vault balance snapshots derived from existing
    /// `swap.v1` parquet partitions. One-shot backfill — reads swap
    /// rows from `dataset/{venue}/swaps/v1/pool=ADDR/year=Y/month=M/
    /// day=D.parquet`, picks the first signature per hour, and
    /// fetches each tx's `preTokenBalances` via proxy-routed
    /// `getTransaction(jsonParsed)`.
    PoolSnapshots(pool_snapshots_cmd::PoolSnapshotsArgs),
    /// One-shot snapshot of Kamino Klend xStock Reserve account
    /// configs (LTV / liquidation threshold / heuristic band /
    /// scope feed wiring + raw account bytes for forensic re-decode).
    KaminoReserves(kamino_reserves_cmd::ReservesArgs),
    /// Weekly snapshot of the Klend borrower book — every Obligation
    /// account in the configured lending market. Writes parent
    /// (per-obligation summary) + child (per-deposit/per-borrow)
    /// schemas to dataset/kamino/obligations/v1 and
    /// dataset/kamino/obligation_positions/v1.
    KaminoObligations(kamino_obligations_cmd::ObligationsArgs),
    /// Periodic snapshot of the Loopscale credit book — every Loan
    /// account, with xStock-collateral flagging. Writes parent + child
    /// schemas to dataset/loopscale/loans/v1 and
    /// dataset/loopscale/loan_collaterals/v1.
    LoopscaleLoans(loopscale_loans_cmd::LoopscaleLoansArgs),
    /// Drift Protocol liquidation events panel — perp / spot /
    /// perp_with_fill / perp_bankruptcy / spot_bankruptcy IXes
    /// decoded into one row per matching IX. Writes to
    /// dataset/drift/liquidations/v1/year=Y/month=M/day=D.parquet.
    DriftLiquidations(drift_liquidations_cmd::DriftLiquidationsArgs),
    /// Mango v4 liquidation events panel — 10 IXes from the IDL
    /// (token + perp + force-cancel-orders variants) decoded into
    /// one row per matching IX. Writes to
    /// dataset/mango_v4/liquidations/v1/year=Y/month=M/day=D.parquet.
    MangoV4Liquidations(mango_v4_liquidations_cmd::MangoV4LiquidationsArgs),
    /// Mango v4 per-market oracle-config snapshot — one
    /// `getProgramAccounts` for Bank, one for PerpMarket, both
    /// filtered by the parent Group pubkey. Writes to
    /// dataset/mango_v4/oracle_configs/v1/year=Y/month=M/day=D.parquet.
    MangoV4OracleConfigs(mango_v4_oracle_configs_cmd::MangoV4OracleConfigsArgs),
    /// Cross-DEX xStock swap prints. Vault-delta extraction across
    /// every DEX touching xStock mints (Orca/Meteora/Phoenix/Raydium
    /// variants/aggregator-routed). Writes to dataset/dex_xstock/
    /// swaps/v1/symbol={X}/year=Y/month=M/day=D.parquet.
    DexXstockSwaps(dex_xstock_swaps_cmd::DexXstockSwapsArgs),
    /// Pyth per-publisher tape — polls Pythnet RPC's
    /// `getMultipleAccounts` for the 32 xStock equity-feed
    /// PriceAccounts and decodes each comp[] into one row per
    /// (feed, publisher) tuple per poll.
    PythPublisher(pyth_publisher_cmd::PythPublisherArgs),
    /// Jito Block Engine bundle-attachment enrichment over an existing
    /// liquidation panel. Reads `(signature, slot, block_time)` from
    /// kamino_liquidation.v1 / jupiter_lend_liquidation.v1 parquet,
    /// queries `bundles/transaction/{sig}`, and writes one
    /// `jito_bundles.v1` row per signature.
    JitoBundles(jito_cmd::JitoBundlesArgs),
    /// Per-slot block-walk priority-fee + Jito-tip percentile panel.
    /// On-demand window walker: emits one
    /// `solana_priority_fees.v1::Stats` row per non-skipped slot in
    /// `[start, end]` (or `[around-window, around+window]`, or
    /// `latest-N`). Writes to dataset/solana/priority_fees/v1/year=Y/
    /// month=M/day=D.parquet.
    PriorityFees(priority_fees_cmd::PriorityFeesArgs),
    /// Single-tick poll of `bundles.jito.wtf/api/v1/bundles/tip_floor`
    /// — the Jito chain-wide rolling tip-percentile distribution.
    /// Writes one `jito_tip_floor.v1::Tick` row to
    /// dataset/jito/tip_floor/v1/year=Y/month=M/day=D.parquet.
    /// Schedule via launchd / cron at the desired cadence (typical:
    /// 10s). Dedups naturally on the upstream `time` field.
    JitoTipFloor(jito_tip_floor_cmd::JitoTipFloorArgs),
    /// Cross-source oracle observation enrichment. Pure offline join
    /// of liquidation events against the four continuously-collected
    /// oracle/price tapes (kamino_scope, pyth, v5_tape's chainlink +
    /// jupiter_mid, redstone). Emits one oracle_context.v1 row per
    /// (event, source[, session]) triple within ±window_secs.
    OracleContext(oracle_context_cmd::OracleContextArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env before clap parses so that env-bound flags like
    // `--helius-api-key` (`#[arg(env = "HELIUS_API_KEY")]`) resolve.
    let _ = dotenvy::dotenv();

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
            SolanaTarget::FluidVaultConfigs(a) => solana_cmd::run_fluid_vault_configs(a).await,
            SolanaTarget::KaminoScopeTape(a) => solana_cmd::run_kamino_scope_tape(a).await,
            SolanaTarget::PoolSnapshots(a) => pool_snapshots_cmd::run_pool_snapshots(a).await,
            SolanaTarget::KaminoReserves(a) => kamino_reserves_cmd::run_reserves(a).await,
            SolanaTarget::KaminoObligations(a) => kamino_obligations_cmd::run_obligations(a).await,
            SolanaTarget::LoopscaleLoans(a) => loopscale_loans_cmd::run_loopscale_loans(a).await,
            SolanaTarget::DriftLiquidations(a) => drift_liquidations_cmd::run_drift_liquidations(a).await,
            SolanaTarget::MangoV4Liquidations(a) => mango_v4_liquidations_cmd::run_mango_v4_liquidations(a).await,
            SolanaTarget::MangoV4OracleConfigs(a) => mango_v4_oracle_configs_cmd::run_mango_v4_oracle_configs(a).await,
            SolanaTarget::DexXstockSwaps(a) => dex_xstock_swaps_cmd::run_dex_xstock_swaps(a).await,
            SolanaTarget::PythPublisher(a) => pyth_publisher_cmd::run_pyth_publisher(a).await,
            SolanaTarget::JitoBundles(a) => jito_cmd::run_jito_bundles(a).await,
            SolanaTarget::JitoTipFloor(a) => jito_tip_floor_cmd::run_jito_tip_floor(a).await,
            SolanaTarget::PriorityFees(a) => priority_fees_cmd::run_priority_fees(a).await,
            SolanaTarget::OracleContext(a) => oracle_context_cmd::run_oracle_context(a).await,
        },
        Command::Redstone(c) => match c.target {
            RedstoneTarget::Tape(a) => redstone_cmd::run_tape(a).await,
        },
        Command::Pyth(c) => match c.target {
            PythTarget::Tape(a) => pyth_cmd::run_tape(a).await,
        },
        Command::Dexagg(c) => match c.target {
            DexaggTarget::GtTrades(a) => dexagg_cmd::run_gt_trades(a).await,
        },
        Command::V5tape(c) => match c.target {
            V5tapeTarget::Tape(a) => v5_cmd::run_tape(a).await,
        },
        Command::Equities(c) => match c.target {
            EquitiesTarget::Bars(a) => equities_cmd::run_bars(a).await,
            EquitiesTarget::Earnings(a) => equities_cmd::run_earnings(a).await,
        },
        Command::Rss(c) => match c.target {
            RssTarget::Backed(a) => rss_cmd::run_backed(a).await,
            RssTarget::NasdaqHalts(a) => rss_cmd::run_nasdaq_halts(a).await,
        },
        Command::Fred(c) => match c.target {
            FredTarget::MacroCalendar(a) => fred_cmd::run_macro_calendar(a).await,
            FredTarget::Series(a) => fred_cmd::run_series(a).await,
        },
        Command::Databento(c) => match c.target {
            DatabentoTarget::Intraday1m(a) => databento_cmd::run_intraday(a).await,
            DatabentoTarget::EquitiesDaily(a) => databento_cmd::run_equities_daily(a).await,
        },
        Command::Deribit(c) => match c.target {
            DeribitTarget::Dvol(a) => deribit_cmd::run_dvol(a).await,
        },
        Command::CexFunding(c) => match c.target {
            CexFundingTarget::Multi(a) => cex_funding_cmd::run_multi(a).await,
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
