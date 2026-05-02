//! `scry` — scryer CLI.
//!
//! Subcommands:
//!
//!   scry import swaps  --input PATH --venue VENUE --pool POOL [--source LABEL] [--dataset DIR]
//!   scry import trades --input PATH --venue VENUE --pair PAIR [--source LABEL] [--dataset DIR]
//!   scry solana swaps  --pool-metadata FILE --start DATE --end DATE
//!                      --proxy-url URL --helius-api-key KEY
//!                      [--dataset DIR] [--venue VENUE]
//!   scry kraken trades --pair PAIR --start DATE --end DATE
//!                      [--source LABEL] [--rate-limit-ms 1000] [--dataset DIR]

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand};

mod backed_cmd;
mod cboe_cmd;
mod cex_funding_cmd;
mod dataset_default;
mod cex_stock_perp_cmd;
mod chainlink_reports_cmd;
mod databento_cmd;
mod deribit_cmd;
mod dex_xstock_swaps_cmd;
mod dexagg_cmd;
mod drift_liquidations_cmd;
mod equities_cmd;
mod evm_cmd;
mod fred_cmd;
mod freshness_cmd;
mod import_cmd;
mod jito_bundle_tape_cmd;
mod jito_cmd;
mod jito_tip_floor_cmd;
mod kamino_obligations_cmd;
mod kamino_reserves_cmd;
mod kraken_cmd;
mod loopscale_loans_cmd;
mod mango_v4_liquidations_cmd;
mod mango_v4_oracle_configs_cmd;
mod marginfi_reserves_cmd;
mod oracle_context_cmd;
mod pool_snapshots_cmd;
mod priority_fees_cmd;
mod pyth_backfill_cmd;
mod pyth_cmd;
mod pyth_poster_cmd;
mod pyth_publisher_cmd;
mod redstone_cmd;
mod rss_cmd;
mod sec_cmd;
mod solana_cmd;
mod v5_cmd;
mod xstock_holders_cmd;

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
    /// SEC EDGAR public-data fetcher. Per-ticker 8-K filing index
    /// from `data.sec.gov/submissions/CIK*.json`. Requires a
    /// User-Agent header per SEC fair-access policy.
    Sec(SecCmd),
    /// Backed Finance xStocks public-API fetcher. v1 surface:
    /// `nav-strikes` continuous indicative-quote tape from
    /// `api.xstocks.fi/api/v2/public/*`. Public, no auth.
    Backed(BackedCmd),
    /// EVM lending-protocol liquidation panel. Aave V3 (Ethereum,
    /// Arbitrum) + Spark (Ethereum) via `eth_getLogs`. Writes to
    /// dataset/evm/liquidations/v1/chain={X}/year=Y/month=M/day=D.parquet.
    Evm(EvmCmd),
    /// Databento Historical API — CME futures 1-minute OHLCV bars.
    /// Pay-as-you-go against the operator's $125 signup credit.
    Databento(DatabentoCmd),
    /// CBOE public-CSV index fetcher — VIX-family + SKEW historical
    /// daily bars from cdn.cboe.com. Writes to
    /// dataset/cboe/indices/v1/index={X}/year=YYYY.parquet.
    Cboe(CboeCmd),
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
    /// Kraken public REST clients. v0.1: `trades` only. Paginating
    /// nanosecond-cursor walk over `api.kraken.com/0/public/Trades`.
    /// Writes to dataset/kraken/trades/v1/pair={PAIR}/year=Y/month=M/
    /// day=D.parquet. Phase 74 (closes v0.1 scope-2).
    Kraken(KrakenCmd),
    /// Multi-venue 24/7 CEX perp tape on xStock underliers. v1 ships
    /// 4 venues (Kraken Futures, Gate.io, OKX, Coinbase International).
    /// Writes to dataset/cex_stock_perp/tape/v1/underlier={SYM}/year=Y/
    /// month=M/day=D.parquet.
    CexStockPerp(CexStockPerpCmd),
    /// Pyth equity-feed poster — write-side daemon (item 44). Fetches
    /// signed Hermes VAAs for SPY/QQQ/AAPL/etc. and posts them to
    /// Solana's existing Pyth receiver program. Methodology:
    /// `methodology_log.md` "Write-side daemons" + "Write-side daemon
    /// schemas". Slice 2: --once + --dry-run only; slice 2c lands the
    /// real on-chain submitter.
    PythPoster(pyth_poster_cmd::PythPosterArgs),
    /// Forward-poll freshness watchdog (phase 70-A). Walks each
    /// expected tape's dataset subtree, finds the newest parquet by
    /// mtime, and exits non-zero if any tape is stale relative to its
    /// per-tape threshold. Schedule via launchd at 15-min cadence so
    /// silent daemon outages surface as a non-zero `launchctl list`
    /// exit code + (optionally) a macOS notification.
    Freshness(freshness_cmd::FreshnessArgs),
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
    /// Import Flipside Crypto Solana DEX swaps (`solana.defi.fact_swaps`
    /// shape) into `swap.v1`. LVR-unblock pivot from Helius
    /// parseTransactions for windows where Helius credit cost is
    /// prohibitive (e.g., 180d Raydium SOL/USDC). Phase 75.
    FlipsideSwaps(import_cmd::FlipsideSwapsArgs),
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
struct SecCmd {
    #[command(subcommand)]
    target: SecTarget,
}

#[derive(Parser, Debug)]
struct BackedCmd {
    #[command(subcommand)]
    target: BackedTarget,
}

#[derive(Subcommand, Debug)]
enum BackedTarget {
    /// Single-tick poll of Backed's per-xStock indicative quote
    /// (`api.xstocks.fi/api/v2/public/assets/{symbol}/price-data`)
    /// plus per-symbol multiplier + halt-status enrichment.
    /// Schedule via launchd at the desired cadence (typical: 60s
    /// during US market hours).
    NavStrikes(backed_cmd::NavStrikesArgs),
}

#[derive(Parser, Debug)]
struct EvmCmd {
    #[command(subcommand)]
    target: EvmTarget,
}

#[derive(Subcommand, Debug)]
enum EvmTarget {
    /// Aave V3 / Spark `LiquidationCall` event walker. One row per
    /// matching event over `[from-block, to-block]` (or
    /// `--lookback-blocks` from current head).
    Liquidations(evm_cmd::LiquidationsArgs),
}

#[derive(Subcommand, Debug)]
enum SecTarget {
    /// Pull the 8-K filing index for the configured tickers from
    /// SEC EDGAR. Writes one
    /// `edgar_8k.v1::Filing` row per 8-K (or 8-K/A) per ticker.
    /// Idempotent — accession_number is the dedup key.
    Edgar8k(sec_cmd::Edgar8kArgs),
}

#[derive(Parser, Debug)]
struct DatabentoCmd {
    #[command(subcommand)]
    target: DatabentoTarget,
}

#[derive(Parser, Debug)]
struct CboeCmd {
    #[command(subcommand)]
    target: CboeTarget,
}

#[derive(Subcommand, Debug)]
enum CboeTarget {
    /// Pull historical daily bars for the configured CBOE indices
    /// (default: VIX,VIX9D,VIX1D,VIX3M,VIX6M,SKEW). One row per
    /// (index, date) pair.
    Indices(cboe_cmd::IndicesArgs),
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

#[derive(Parser, Debug)]
struct KrakenCmd {
    #[command(subcommand)]
    target: KrakenTarget,
}

#[derive(Subcommand, Debug)]
enum KrakenTarget {
    /// Window-walker over Kraken's public `Trades` endpoint. Iterates
    /// `[--start, --end)` via the upstream's nanosecond cursor at
    /// 1 req/s sustained, dedups on `kraken:{trade_id}`, writes to
    /// dataset/kraken/trades/v1/pair={PAIR}/year=Y/month=M/day=D.parquet.
    Trades(kraken_cmd::TradesArgs),
}

#[derive(Parser, Debug)]
struct CexStockPerpCmd {
    #[command(subcommand)]
    target: CexStockPerpTarget,
}

#[derive(Subcommand, Debug)]
enum CexStockPerpTarget {
    /// Single-tick poll across the configured venues for the
    /// configured xStock underlier set. Schedule via launchd at
    /// the desired cadence (typical: 60s).
    Tape(cex_stock_perp_cmd::TapeArgs),
    /// 1-minute OHLCV bars per venue per stock-perp. Companion
    /// forward tape for paper 1's §1.2 weekday-vs-weekend volume
    /// DiD. Writes to dataset/cex_stock_perp/ohlcv/v1/underlier=
    /// {SYM}/year=Y/month=M/day=D.parquet.
    Ohlcv(cex_stock_perp_cmd::OhlcvArgs),
    /// Kraken Futures historical 1m OHLCV backfill — walks the
    /// `[start, end]` window in chunks (Kraken caps at 2000 bars
    /// per call ≈ 1.39 days at 1m) and writes to the same dataset
    /// as the forward `ohlcv` tape. Only Kraken Futures exposes
    /// deep history per `PF_*XUSD` listing date; other venues cap
    /// at ~30-90 days and rely on the forward tape.
    Backfill(cex_stock_perp_cmd::BackfillArgs),
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
    /// Per-symbol corporate-action history (splits + dividends) from
    /// Yahoo's `chart` endpoint with `events=div|split`. Writes one
    /// yearly parquet per symbol under dataset/yahoo/corp_actions/v1/.
    /// Soothsayer Paper-1 §10.2 OOS DQ-panel filter.
    CorpActions(equities_cmd::CorpActionsArgs),
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
    /// Historical backfill via the Pyth Benchmarks API
    /// (`benchmarks.pyth.network/v1/updates/price/{ts}/{interval}`).
    /// Iterates `[--start, --end)` at minute boundaries, picks the
    /// latest publish per (feed, 60s bucket), writes one
    /// `pyth.v1::Reading` per feed-with-data-in-bucket. Off-hours
    /// session feeds emit no row (intrinsic to Pyth's session-feed
    /// design — outer-join on the consumer side). Phase 67 (item 49a).
    Backfill(pyth_backfill_cmd::BackfillArgs),
}

#[derive(Subcommand, Debug)]
enum DexaggTarget {
    /// GeckoTerminal pool-trades poll (free-tier returns latest ~300
    /// trades). Schedule via launchd / cron (typical: 15m). Idempotent
    /// — re-runs within the trade-coverage window dedup cleanly on
    /// `tx_hash`.
    GtTrades(dexagg_cmd::GtTradesArgs),
    /// Raydium v3 API pool-metadata one-shot. Replaces
    /// `quant-work/lvr/find_pool.py`. Outputs both parquet (for
    /// time-series snapshot drift) and the consumer-shape JSON
    /// (`quant-work/data/pool_metadata.json`).
    RaydiumPoolMetadata(dexagg_cmd::RaydiumPoolMetadataArgs),
    /// GeckoTerminal historical OHLCV bars (free-tier, ~100-182
    /// daily bars per pool per call). Replaces
    /// `quant-work/lst/fetch_gt_ohlcv.py`. Forward-accumulating
    /// tape — re-runs at any cadence dedup cleanly.
    GtOhlcv(dexagg_cmd::GtOhlcvArgs),
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
    /// Fetch swaps via Helius enhanced `/v0/addresses/{pool}/transactions
    /// ?type=SWAP` (server-side swap-only filter, ~100 swaps per call).
    /// Phase 78 LVR-unblock pivot — drops the credit cost from ~1
    /// credit per non-swap sig (the phase-4 vault-delta path) to
    /// ~1 credit per CALL, making 180d high-volume-pool backfills
    /// fit a 9M-credit budget. `_source =
    /// "helius:enhanced:transactions:type=SWAP"` distinguishes from
    /// vault-delta rows.
    SwapsHeliusEnhanced(solana_cmd::SwapsHeliusEnhancedArgs),
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
    /// MarginFi-v2 Bank-account snapshot. One-shot
    /// `getProgramAccounts` against `MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA`
    /// with a Bank-disc memcmp filter; per-Bank decode + xStock
    /// post-filter (defaults to xstock-only; pass `--all` to snapshot
    /// every Bank). Writes one `marginfi_reserve.v1::Reserve` row per
    /// matched Bank to dataset/marginfi/reserves/v1/year=Y/month=M/day=D.parquet.
    MarginfiReserves(marginfi_reserves_cmd::MarginfiReservesArgs),
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
    /// Per-slot block-walk for `jito_bundle_tape.v1` — emits zero-or-
    /// more bundle-landing rows per slot via the on-chain heuristic
    /// (lead-tip-paying tx + maximal preceding non-vote run, capped at
    /// MAX_BUNDLE_SIZE=5). Wishlist sub-item 51a; methodology phases
    /// 80 + 81 in `methodology_log.md`. Schedule via launchd
    /// `StartInterval` paired with `--latest-slots` for forward-poll.
    JitoBundleTape(jito_bundle_tape_cmd::JitoBundleTapeArgs),
    /// Per-slot block-walk priority-fee + Jito-tip percentile panel.
    /// On-demand window walker: emits one
    /// `solana_priority_fees.v1::Stats` row per non-skipped slot in
    /// `[start, end]` (or `[around-window, around+window]`, or
    /// `latest-N`). Writes to dataset/solana/priority_fees/v1/year=Y/
    /// month=M/day=D.parquet.
    PriorityFees(priority_fees_cmd::PriorityFeesArgs),
    /// Top-N holders snapshot per xStock mint via
    /// `getTokenLargestAccounts` + per-account owner lookup.
    /// Writes to dataset/xstock/xstock_holders/v1/year=Y/month=M/
    /// day=D.parquet. Schedule weekly via launchd.
    XstockHolders(xstock_holders_cmd::XstockHoldersArgs),
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
    /// Continuous Chainlink Data Streams report tape. Walks the
    /// Verifier program's signature stream, decodes every verify CPI,
    /// emits one `chainlink_data_streams.v1::Report` per
    /// (feed, observation, signature) triple. Third leg (alongside
    /// backed_nav_strikes + cex_stock_perp_tape) for the paper §1.1
    /// oracle-divergence analysis.
    ChainlinkReports(chainlink_reports_cmd::ChainlinkReportsArgs),
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
            ImportTarget::FlipsideSwaps(a) => import_cmd::run_flipside_swaps(a).await,
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
            SolanaTarget::SwapsHeliusEnhanced(a) => solana_cmd::run_swaps_helius_enhanced(a).await,
            SolanaTarget::KaminoLiquidations(a) => solana_cmd::run_kamino_liquidations(a).await,
            SolanaTarget::JupiterLendLiquidations(a) => {
                solana_cmd::run_jupiter_lend_liquidations(a).await
            }
            SolanaTarget::FluidVaultConfigs(a) => solana_cmd::run_fluid_vault_configs(a).await,
            SolanaTarget::KaminoScopeTape(a) => solana_cmd::run_kamino_scope_tape(a).await,
            SolanaTarget::PoolSnapshots(a) => pool_snapshots_cmd::run_pool_snapshots(a).await,
            SolanaTarget::KaminoReserves(a) => kamino_reserves_cmd::run_reserves(a).await,
            SolanaTarget::KaminoObligations(a) => kamino_obligations_cmd::run_obligations(a).await,
            SolanaTarget::LoopscaleLoans(a) => loopscale_loans_cmd::run_loopscale_loans(a).await,
            SolanaTarget::DriftLiquidations(a) => {
                drift_liquidations_cmd::run_drift_liquidations(a).await
            }
            SolanaTarget::MangoV4Liquidations(a) => {
                mango_v4_liquidations_cmd::run_mango_v4_liquidations(a).await
            }
            SolanaTarget::MangoV4OracleConfigs(a) => {
                mango_v4_oracle_configs_cmd::run_mango_v4_oracle_configs(a).await
            }
            SolanaTarget::MarginfiReserves(a) => {
                marginfi_reserves_cmd::run_marginfi_reserves(a).await
            }
            SolanaTarget::DexXstockSwaps(a) => dex_xstock_swaps_cmd::run_dex_xstock_swaps(a).await,
            SolanaTarget::PythPublisher(a) => pyth_publisher_cmd::run_pyth_publisher(a).await,
            SolanaTarget::JitoBundles(a) => jito_cmd::run_jito_bundles(a).await,
            SolanaTarget::JitoBundleTape(a) => jito_bundle_tape_cmd::run_jito_bundle_tape(a).await,
            SolanaTarget::JitoTipFloor(a) => jito_tip_floor_cmd::run_jito_tip_floor(a).await,
            SolanaTarget::PriorityFees(a) => priority_fees_cmd::run_priority_fees(a).await,
            SolanaTarget::XstockHolders(a) => xstock_holders_cmd::run_xstock_holders(a).await,
            SolanaTarget::OracleContext(a) => oracle_context_cmd::run_oracle_context(a).await,
            SolanaTarget::ChainlinkReports(a) => {
                chainlink_reports_cmd::run_chainlink_reports(a).await
            }
        },
        Command::Redstone(c) => match c.target {
            RedstoneTarget::Tape(a) => redstone_cmd::run_tape(a).await,
        },
        Command::Pyth(c) => match c.target {
            PythTarget::Tape(a) => pyth_cmd::run_tape(a).await,
            PythTarget::Backfill(a) => pyth_backfill_cmd::run_backfill(a).await,
        },
        Command::Dexagg(c) => match c.target {
            DexaggTarget::GtTrades(a) => dexagg_cmd::run_gt_trades(a).await,
            DexaggTarget::RaydiumPoolMetadata(a) => dexagg_cmd::run_raydium_pool_metadata(a).await,
            DexaggTarget::GtOhlcv(a) => dexagg_cmd::run_gt_ohlcv(a).await,
        },
        Command::V5tape(c) => match c.target {
            V5tapeTarget::Tape(a) => v5_cmd::run_tape(a).await,
        },
        Command::Equities(c) => match c.target {
            EquitiesTarget::Bars(a) => equities_cmd::run_bars(a).await,
            EquitiesTarget::Earnings(a) => equities_cmd::run_earnings(a).await,
            EquitiesTarget::CorpActions(a) => equities_cmd::run_corp_actions(a).await,
        },
        Command::Rss(c) => match c.target {
            RssTarget::Backed(a) => rss_cmd::run_backed(a).await,
            RssTarget::NasdaqHalts(a) => rss_cmd::run_nasdaq_halts(a).await,
        },
        Command::Fred(c) => match c.target {
            FredTarget::MacroCalendar(a) => fred_cmd::run_macro_calendar(a).await,
            FredTarget::Series(a) => fred_cmd::run_series(a).await,
        },
        Command::Sec(c) => match c.target {
            SecTarget::Edgar8k(a) => sec_cmd::run_edgar_8k(a).await,
        },
        Command::Backed(c) => match c.target {
            BackedTarget::NavStrikes(a) => backed_cmd::run_nav_strikes(a).await,
        },
        Command::Evm(c) => match c.target {
            EvmTarget::Liquidations(a) => evm_cmd::run_liquidations(a).await,
        },
        Command::Databento(c) => match c.target {
            DatabentoTarget::Intraday1m(a) => databento_cmd::run_intraday(a).await,
            DatabentoTarget::EquitiesDaily(a) => databento_cmd::run_equities_daily(a).await,
        },
        Command::Cboe(c) => match c.target {
            CboeTarget::Indices(a) => cboe_cmd::run_indices(a).await,
        },
        Command::Deribit(c) => match c.target {
            DeribitTarget::Dvol(a) => deribit_cmd::run_dvol(a).await,
        },
        Command::CexStockPerp(c) => match c.target {
            CexStockPerpTarget::Tape(a) => cex_stock_perp_cmd::run_tape(a).await,
            CexStockPerpTarget::Ohlcv(a) => cex_stock_perp_cmd::run_ohlcv(a).await,
            CexStockPerpTarget::Backfill(a) => cex_stock_perp_cmd::run_backfill(a).await,
        },
        Command::CexFunding(c) => match c.target {
            CexFundingTarget::Multi(a) => cex_funding_cmd::run_multi(a).await,
        },
        Command::Kraken(c) => match c.target {
            KrakenTarget::Trades(a) => kraken_cmd::run_trades(a).await,
        },
        Command::PythPoster(a) => pyth_poster_cmd::run_pyth_poster(a).await,
        Command::Freshness(a) => freshness_cmd::run_freshness(a).await,
    }
}

/// Parse `YYYY-MM-DD` (UTC midnight) or full RFC 3339 into a unix
/// seconds timestamp. The first form is what scripts already use; the
/// second is for second-precision windows in tests.
pub fn parse_unix_seconds(s: &str) -> Result<i64> {
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).context("invalid time-of-day")?);
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
