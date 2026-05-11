//! `DatasetSchema` trait — abstract over per-schema arrow + partition
//! semantics so `Dataset::write` / `read` work generically.
//!
//! The trait captures the variation across the 5 v0.1 schemas:
//!
//! - `DATA_TYPE` (e.g., `"swaps"`, `"trades"`, `"oracle_tape"`,
//!   `"tape"`) — the path segment under `{venue}/`.
//! - `SCHEMA_MAJOR` — the major version that goes into the `v{N}/`
//!   path segment.
//! - `PARTITION_KEY_PREFIX` — `Some("pool")` for swap.v1,
//!   `Some("pair")` for trade.v1, `None` for the no-key oracle
//!   tapes (kamino_scope, pyth, v5_tape).
//! - `ts_unix_seconds(&self)` — extracts the partitioning timestamp
//!   from each row regardless of the schema's `ts` field name or
//!   type (some schemas have `ts: i64`, some `ts: f64`, some
//!   `poll_unix: i64`, some `scope_unix_ts: i64`).
//! - `dedup_key(&self)` — already exists per-schema; the trait
//!   surfaces it for the store layer.
//! - `to_record_batch` / `from_record_batch` — delegate to the
//!   existing module-scoped functions in `scryer-schema`.

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use scryer_schema::{
    backed, backed_nav_strikes, bo_intraday_1m, cboe_indices, cex_perp_funding_multi, cex_stock_perp_ohlcv, cex_stock_perp_tape, chainlink_data_streams, clmm_pool_state, cme_intraday_1m, dead_letter, deribit_iv, dex_xstock_swaps, dlmm_pool_state, drift_liquidation, earnings, edgar_8k, evm_liquidation, fluid_vault_config, freshness_check, fred_macro, fred_macro_extended, geckoterminal, geckoterminal_ohlcv,
    jito_bundle_tape, jito_bundles, jito_tip_floor, jupiter_lend_liquidation,
    kamino_liquidation, kamino_obligation,
    kamino_obligation_position, kamino_reserve, kamino_scope, kraken_funding, loopscale_loan,
    loopscale_loan_collateral, mango_v4_liquidation, mango_v4_oracle_config, marginfi_liquidation, marginfi_reserve,
    nasdaq_halts, nasdaq_halts_intraday,
    oracle_context, oracle_pyth_lazer_tape, oracle_soothsayer_v6_band_tape,
    pool_snapshot, pyth, pyth_poster_post, pyth_poster_tx, pyth_publisher,
    raydium_pool_metadata, redstone, single_stock_iv, solana_priority_fees, swap, trade, v5_tape, validator_client, workflow_run, workflow_run_summary, xstock_holders, yahoo, yahoo_corp_actions, FromArrowError,
};

/// Time granularity of a dataset's partitioning. Each schema picks
/// the granularity that right-sizes its partition files: too-fine
/// produces tiny per-row files (wasted inodes, slow scans), too-coarse
/// produces multi-GB files that strain memory at write/merge time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionGranularity {
    /// `year=YYYY/month=MM/day=DD.parquet`. Used by event-stream data
    /// (swaps, trades, oracle tapes) where each day has 1k+ rows.
    Daily,
    /// `year=YYYY/month=MM.parquet`. Used by monthly-keyed periodic
    /// data (Kraken Pro Futures funding rates: one row per hour per
    /// symbol → ~720 rows/month/symbol).
    Monthly,
    /// `year=YYYY.parquet`. Used by daily-bar data (Yahoo OHLCV) and
    /// other low-frequency keyed datasets where ~250 rows/year would
    /// produce single-row files at daily granularity.
    Yearly,
}

/// Concrete time partition that maps directly to a partition path
/// segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PartitionTime {
    Daily(crate::partition::UtcDay),
    Monthly { year: i32, month: u32 },
    Yearly(i32),
}

impl PartitionTime {
    pub fn granularity(&self) -> PartitionGranularity {
        match self {
            Self::Daily(_) => PartitionGranularity::Daily,
            Self::Monthly { .. } => PartitionGranularity::Monthly,
            Self::Yearly(_) => PartitionGranularity::Yearly,
        }
    }
}

impl From<crate::partition::UtcDay> for PartitionTime {
    fn from(d: crate::partition::UtcDay) -> Self {
        Self::Daily(d)
    }
}

pub trait DatasetSchema: Sized + Clone {
    /// Path segment between `{venue}/` and `v{N}/`.
    const DATA_TYPE: &'static str;
    /// Major schema version for the `v{N}/` partition segment.
    const SCHEMA_MAJOR: u32 = 1;
    /// `Some("pool")` for keyed schemas, `None` for event-stream.
    const PARTITION_KEY_PREFIX: Option<&'static str>;
    /// Time granularity of the partition path. `Daily` is the default
    /// (used by all v0.1 schemas through Phase 10); `Yearly` lands in
    /// Phase 11 for daily-bar / low-frequency data.
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    /// Used by the store to bucket each row into a UTC-day partition.
    fn ts_unix_seconds(&self) -> i64;

    /// Stable per-row identifier for read-modify-write dedup.
    fn dedup_key(&self) -> String;

    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError>;
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError>;
}

impl DatasetSchema for swap::v1::Swap {
    const DATA_TYPE: &'static str = "swaps";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("pool");

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        swap::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        swap::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for trade::v1::Trade {
    const DATA_TYPE: &'static str = "trades";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("pair");

    fn ts_unix_seconds(&self) -> i64 {
        // trade.v1's ts is f64 seconds; truncate for date bucketing.
        self.ts as i64
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        trade::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        trade::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for kamino_scope::v1::Reading {
    const DATA_TYPE: &'static str = "oracle_tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;

    fn ts_unix_seconds(&self) -> i64 {
        self.scope_unix_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        kamino_scope::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        kamino_scope::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for chainlink_data_streams::v1::Report {
    const DATA_TYPE: &'static str = "data_streams";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // observation_ts is the DON-side observation second; this is
        // the cadence anchor and what we want to bucket the row by.
        self.observation_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        chainlink_data_streams::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        chainlink_data_streams::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for pyth_publisher::v1::Submission {
    const DATA_TYPE: &'static str = "publisher_tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Use observation_unix_ts when set; else fall back to
        // _fetched_at (early Pythnet snapshots may emit 0 here).
        if self.observation_unix_ts > 0 {
            self.observation_unix_ts
        } else {
            self.meta.fetched_at
        }
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        pyth_publisher::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        pyth_publisher::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for pyth::v1::Reading {
    const DATA_TYPE: &'static str = "oracle_tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;

    fn ts_unix_seconds(&self) -> i64 {
        self.poll_unix
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        pyth::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        pyth::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for redstone::v1::Reading {
    const DATA_TYPE: &'static str = "oracle_tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;

    fn ts_unix_seconds(&self) -> i64 {
        // redstone_ts is microseconds since unix epoch.
        self.redstone_ts / 1_000_000
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        redstone::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        redstone::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for fluid_vault_config::v1::Config {
    const DATA_TYPE: &'static str = "vault_configs";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // Snapshot data has no inherent timestamp — partition by the
        // _fetched_at year (when we observed the on-chain state).
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        fluid_vault_config::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        fluid_vault_config::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for jupiter_lend_liquidation::v1::Liquidation {
    const DATA_TYPE: &'static str = "liquidations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        jupiter_lend_liquidation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        jupiter_lend_liquidation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for kamino_liquidation::v1::Liquidation {
    const DATA_TYPE: &'static str = "liquidations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        kamino_liquidation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        kamino_liquidation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for jito_bundles::v1::Bundle {
    const DATA_TYPE: &'static str = "bundles";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        jito_bundles::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        jito_bundles::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for mango_v4_liquidation::v1::Liquidation {
    const DATA_TYPE: &'static str = "liquidations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        mango_v4_liquidation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        mango_v4_liquidation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for mango_v4_oracle_config::v1::OracleSnapshot {
    const DATA_TYPE: &'static str = "oracle_configs";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.snapshot_unix_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        mango_v4_oracle_config::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        mango_v4_oracle_config::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for solana_priority_fees::v1::Stats {
    const DATA_TYPE: &'static str = "priority_fees";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        solana_priority_fees::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        solana_priority_fees::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for jito_tip_floor::v1::Tick {
    const DATA_TYPE: &'static str = "tip_floor";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        jito_tip_floor::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        jito_tip_floor::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for jito_bundle_tape::v1::BundleLanding {
    const DATA_TYPE: &'static str = "bundle_tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        jito_bundle_tape::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        jito_bundle_tape::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for clmm_pool_state::v1::PoolState {
    const DATA_TYPE: &'static str = "clmm_pool_state";
    /// Partition by `dex_program` per the schema doc — Whirlpool and
    /// Raydium-CLMM accounts decode through different decoders and
    /// run as separate fetcher daemons; keying the partition by DEX
    /// keeps the file boundary at the natural per-daemon boundary.
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("dex");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        clmm_pool_state::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        clmm_pool_state::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for dlmm_pool_state::v1::PoolState {
    const DATA_TYPE: &'static str = "dlmm_pool_state";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        dlmm_pool_state::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        dlmm_pool_state::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for validator_client::v1::ClientLabel {
    const DATA_TYPE: &'static str = "client_label";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // Per-epoch metadata snapshot — partition by `_fetched_at`
        // (the snapshot timestamp). ~180 epochs × ~1500 leaders =
        // ~270K rows/year (per schema doc), well-suited to yearly
        // partitions.
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        validator_client::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        validator_client::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for kamino_obligation::v1::Obligation {
    const DATA_TYPE: &'static str = "obligations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Snapshot data — partition by `_fetched_at` (the snapshot
        // timestamp). Daily granularity captures weekly cadence
        // naturally: each snapshot day is its own parquet file.
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        kamino_obligation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        kamino_obligation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for loopscale_loan::v1::Loan {
    const DATA_TYPE: &'static str = "loans";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        loopscale_loan::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        loopscale_loan::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for loopscale_loan_collateral::v1::Collateral {
    const DATA_TYPE: &'static str = "loan_collaterals";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        loopscale_loan_collateral::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        loopscale_loan_collateral::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for kamino_obligation_position::v1::Position {
    const DATA_TYPE: &'static str = "obligation_positions";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        kamino_obligation_position::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        kamino_obligation_position::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for cex_stock_perp_ohlcv::v1::Bar {
    const DATA_TYPE: &'static str = "ohlcv";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("underlier");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.bar_open_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        cex_stock_perp_ohlcv::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        cex_stock_perp_ohlcv::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for cex_stock_perp_tape::v1::Tick {
    const DATA_TYPE: &'static str = "tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("underlier");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        cex_stock_perp_tape::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        cex_stock_perp_tape::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for cex_perp_funding_multi::v1::Rate {
    const DATA_TYPE: &'static str = "funding";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.funding_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        cex_perp_funding_multi::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        cex_perp_funding_multi::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for drift_liquidation::v1::Liquidation {
    const DATA_TYPE: &'static str = "liquidations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        drift_liquidation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        drift_liquidation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for cme_intraday_1m::v1::Bar {
    const DATA_TYPE: &'static str = "intraday_1m";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        cme_intraday_1m::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        cme_intraday_1m::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for bo_intraday_1m::v1::Bar {
    /// `dataset/blue_ocean/intraday_1m/v1/symbol={SYM}/year=Y/month=M/day=D.parquet`.
    /// Identical row-shape to `cme_intraday_1m.v1`, separate venue +
    /// schema id to keep Blue Ocean's overnight-only schedule and
    /// raw-NMS-ticker symbology distinct from CME continuous futures.
    const DATA_TYPE: &'static str = "intraday_1m";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        bo_intraday_1m::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        bo_intraday_1m::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for nasdaq_halts_intraday::v1::Bar {
    const DATA_TYPE: &'static str = "halts_intraday";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        nasdaq_halts_intraday::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        nasdaq_halts_intraday::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for dex_xstock_swaps::v1::Swap {
    const DATA_TYPE: &'static str = "swaps";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        dex_xstock_swaps::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        dex_xstock_swaps::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for fred_macro::v1::Event {
    const DATA_TYPE: &'static str = "macro_calendar";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // Date32 (days-since-epoch) → unix seconds, midnight UTC.
        self.event_date as i64 * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        fred_macro::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        fred_macro::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for geckoterminal_ohlcv::v1::Bar {
    const DATA_TYPE: &'static str = "ohlcv";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("pool");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        geckoterminal_ohlcv::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        geckoterminal_ohlcv::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for evm_liquidation::v1::Liquidation {
    const DATA_TYPE: &'static str = "liquidations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("chain");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_timestamp
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        evm_liquidation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        evm_liquidation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for edgar_8k::v1::Filing {
    const DATA_TYPE: &'static str = "filings_8k";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("ticker");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        self.filing_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        edgar_8k::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        edgar_8k::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for xstock_holders::v1::Holder {
    const DATA_TYPE: &'static str = "xstock_holders";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.snapshot_unix_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        xstock_holders::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        xstock_holders::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for raydium_pool_metadata::v1::PoolMetadata {
    const DATA_TYPE: &'static str = "pool_metadata";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("pool");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        self.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        raydium_pool_metadata::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        raydium_pool_metadata::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for cboe_indices::v1::Bar {
    const DATA_TYPE: &'static str = "indices";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("index");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        self.date as i64 * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        cboe_indices::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        cboe_indices::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for deribit_iv::v1::DvolBar {
    const DATA_TYPE: &'static str = "dvol";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("underlying");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        deribit_iv::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        deribit_iv::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for fred_macro_extended::v1::Observation {
    const DATA_TYPE: &'static str = "macro_extended";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("series");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        self.date as i64 * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        fred_macro_extended::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        fred_macro_extended::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for oracle_context::v1::Observation {
    const DATA_TYPE: &'static str = "observations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.event_block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        oracle_context::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        oracle_context::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for kraken_funding::v1::Rate {
    const DATA_TYPE: &'static str = "funding";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Monthly;

    fn ts_unix_seconds(&self) -> i64 {
        // ts is microseconds since unix epoch.
        self.ts / 1_000_000
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        kraken_funding::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        kraken_funding::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for nasdaq_halts::v1::Halt {
    const DATA_TYPE: &'static str = "halts";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by halt_date year. halt_date is days since
        // unix epoch.
        (self.halt_date as i64) * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        nasdaq_halts::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        nasdaq_halts::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for backed_nav_strikes::v1::Strike {
    const DATA_TYPE: &'static str = "nav_strikes";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.nav_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        backed_nav_strikes::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        backed_nav_strikes::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for backed::v1::Action {
    const DATA_TYPE: &'static str = "corp_actions";
    /// No partition key — `repo` strings contain `/` and the data
    /// volume is small (~13 commits to date). Yearly partitioning by
    /// `commit_date` year keeps the on-disk layout simple.
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by commit_date (the day the corp action commit
        // landed in the GitHub repo), not detected_at (the day we
        // happened to scrape it). Consumer queries are
        // commit-timeline-shaped, not scrape-timeline-shaped.
        (self.commit_date as i64) * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        backed::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        backed::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for earnings::v1::Event {
    const DATA_TYPE: &'static str = "earnings";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // earnings_date is days since unix epoch; year is what we
        // partition by, so seconds at UTC midnight is sufficient.
        (self.earnings_date as i64) * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        earnings::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        earnings::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for yahoo::v1::Bar {
    const DATA_TYPE: &'static str = "equities_daily";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // ts is days since unix epoch (Date32). Convert to unix seconds
        // at UTC midnight of that day.
        (self.ts as i64) * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        yahoo::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        yahoo::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for yahoo_corp_actions::v1::Action {
    const DATA_TYPE: &'static str = "corp_actions";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("symbol");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // event_date is Date32 (days since unix epoch); year is what
        // we partition by, so seconds at UTC midnight is sufficient.
        (self.event_date as i64) * 86_400
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        yahoo_corp_actions::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        yahoo_corp_actions::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for geckoterminal::v1::Trade {
    const DATA_TYPE: &'static str = "trades";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("pool");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        geckoterminal::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        geckoterminal::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for kamino_reserve::v1::Reserve {
    const DATA_TYPE: &'static str = "reserves";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    /// Reserve config rarely changes (Kamino governance is the only
    /// mutator). Yearly partitioning gives 1-N snapshots / year /
    /// reserve, comfortably small per-file.
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Yearly;

    fn ts_unix_seconds(&self) -> i64 {
        // Snapshot data — partition by `_fetched_at` year (when we
        // observed the on-chain state), same convention as
        // `fluid_vault_config`.
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        kamino_reserve::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        kamino_reserve::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for marginfi_liquidation::v1::Liquidation {
    const DATA_TYPE: &'static str = "liquidations";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.block_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        marginfi_liquidation::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        marginfi_liquidation::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for marginfi_reserve::v1::Reserve {
    const DATA_TYPE: &'static str = "reserves";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    /// Bank config rarely changes (MarginFi governance is the only
    /// mutator). Daily partitioning gives 1 snapshot per day per Bank
    /// for the weekly-snapshot cadence; matches `kamino_obligation.v1`'s
    /// daily-snapshot convention rather than `kamino_reserve.v1`'s
    /// yearly — MarginFi has 422+ Banks today (vs Kamino-xStocks' 8)
    /// so per-day-per-Bank rolls up cleaner.
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.meta.fetched_at
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        marginfi_reserve::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        marginfi_reserve::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for pool_snapshot::v1::Snapshot {
    const DATA_TYPE: &'static str = "pool_snapshots";
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("pool");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.hour
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        pool_snapshot::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        pool_snapshot::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for v5_tape::v1::Reading {
    /// `tape` (not `v5_tape`): the experiment iteration is captured
    /// in the venue (`soothsayer_v5`) per the methodology log
    /// "Soothsayer venue versioning" section.
    const DATA_TYPE: &'static str = "tape";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;

    fn ts_unix_seconds(&self) -> i64 {
        self.poll_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        v5_tape::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        v5_tape::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for pyth_poster_post::v1::Post {
    const DATA_TYPE: &'static str = "posts";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by Hermes observation time, not daemon iteration
        // time — keeps the tape "per upstream observation" semantics
        // and lets re-runs over a window land in the right day-files.
        self.hermes_publish_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        pyth_poster_post::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        pyth_poster_post::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for pyth_poster_tx::v1::TxRecord {
    const DATA_TYPE: &'static str = "txs";
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by the parent observation's hermes_publish_time
        // (NOT the tx's confirmed_at_unix) so consumers can join
        // post + tx tapes by `(feed_id_hex, hermes_publish_time)` and
        // the day-partitions line up. Confirmed-time-based partitioning
        // would split a flow's two txs across daily file boundaries on
        // late-confirm runs, breaking the join semantics.
        self.hermes_publish_time
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        pyth_poster_tx::v1::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        pyth_poster_tx::v1::from_record_batch(batch)
    }
}

impl DatasetSchema for freshness_check::v2::FreshnessCheck {
    /// `dataset/internal.scryer/freshness_check/v2/year=Y/month=M/day=D.parquet`.
    /// Daily-granular: each row's `check_at_unix_secs` buckets into
    /// the day the check ran on, so today's snapshot lands in
    /// today's partition and operators query "the freshest check
    /// for each manifest" by max(check_at_unix_secs) within the
    /// current partition.
    const DATA_TYPE: &'static str = "freshness_check";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.check_at_unix_secs
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        freshness_check::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        freshness_check::v2::from_record_batch(batch)
    }
}

impl DatasetSchema for dead_letter::v2::DeadLetter {
    /// `dataset/internal.scryer/dead_letter/v2/year=Y/month=M/day=D.parquet`.
    /// Partition by `triggered_at_unix_secs` so the failed run lands
    /// in the same UTC day partition as the corresponding
    /// workflow_run row — joining the two by run_id stays
    /// partition-aligned.
    const DATA_TYPE: &'static str = "dead_letter";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.triggered_at_unix_secs
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        dead_letter::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        dead_letter::v2::from_record_batch(batch)
    }
}

impl DatasetSchema for workflow_run_summary::v2::WorkflowRunSummary {
    /// `dataset/internal.scryer/workflow_run_summary/v2/year=Y/month=M/day=D.parquet`.
    /// Daily-granular: each row's `summary_date_unix_secs` is the
    /// UTC midnight of the day being summarized, so the partition
    /// path naturally aligns with the summary date.
    const DATA_TYPE: &'static str = "workflow_run_summary";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.summary_date_unix_secs
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        workflow_run_summary::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        workflow_run_summary::v2::from_record_batch(batch)
    }
}

impl DatasetSchema for single_stock_iv::v2::SingleStockIv {
    /// `dataset/volatility.<venue>/single_stock_iv/v2/year=Y/month=M/day=D.parquet`.
    /// Per-venue domain.source goes into the `venue` arg passed to
    /// `Dataset::write` (e.g. `"volatility.yahoo"`); this trait impl
    /// is venue-agnostic since the row carries the schema id in
    /// `_schema_version`.
    const DATA_TYPE: &'static str = "single_stock_iv";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        self.ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        single_stock_iv::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        single_stock_iv::v2::from_record_batch(batch)
    }
}

impl DatasetSchema for workflow_run::v2::WorkflowRun {
    /// `dataset/internal.scryer/workflow_run/v2/year=Y/month=M/day=D.parquet`.
    /// First v2-namespace schema; partition layout follows the
    /// `<domain>.<source>` venue convention locked in
    /// `crates/scryer-store/src/lib.rs::venue::INTERNAL_SCRYER`.
    const DATA_TYPE: &'static str = "workflow_run";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = None;
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by trigger time, not finish time: it's monotonic
        // across retries of the same attempt and answers the
        // operator's "when was this work scheduled" question without
        // post-hoc joins.
        self.triggered_at_unix_secs
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        workflow_run::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        workflow_run::v2::from_record_batch(batch)
    }
}

impl DatasetSchema for oracle_pyth_lazer_tape::v2::Row {
    /// `dataset/oracle.pyth_lazer/tape/v2/feed_id={N}/year=Y/month=M/day=D.parquet`.
    /// Partition by `price_feed_id` (integer), not by `symbol` —
    /// Pyth-canonical symbol strings like `"Equity.US.SPY/USD"`
    /// contain a `/` that would break the partition path layout.
    /// `price_feed_id` is the canonical Lazer identifier, is `/`-free,
    /// and dedupes 1:1 with the symbol string in practice. Consumers
    /// who want the human-readable label read it from the in-row
    /// `symbol` column. Daily granularity matches the manifest's
    /// planned 60s-cycle subscriber pattern.
    const DATA_TYPE: &'static str = "tape";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("feed_id");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by `publish_timestamp_us`, not `_fetched_at`.
        // Re-runs of the same window produce identical
        // (price_feed_id, publish_timestamp_us) tuples and dedup
        // naturally; partitioning by publish-time keeps the file
        // boundary aligned with the publish-timeline question
        // consumers ask.
        self.publish_timestamp_us / 1_000_000
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        oracle_pyth_lazer_tape::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        oracle_pyth_lazer_tape::v2::from_record_batch(batch)
    }
}

impl DatasetSchema for oracle_soothsayer_v6_band_tape::v2::Row {
    /// `dataset/oracle.soothsayer_v6/band_tape/v2/profile={lending,amm}/year=Y/month=M/day=D.parquet`.
    /// Single schema across both Lending and AMM profiles per the
    /// "Soothsayer Lending-track Band Tape — 2026-05-03" methodology
    /// entry; partition key `profile` (lending|amm) splits the two at
    /// write time. Daily granularity matches the publisher cadence
    /// floor (weekly publish; daily polling tightens freshness signal).
    const DATA_TYPE: &'static str = "band_tape";
    const SCHEMA_MAJOR: u32 = 2;
    const PARTITION_KEY_PREFIX: Option<&'static str> = Some("profile");
    const PARTITION_GRANULARITY: PartitionGranularity = PartitionGranularity::Daily;

    fn ts_unix_seconds(&self) -> i64 {
        // Partition by `publish_ts` (the on-chain publish slot's block
        // time), not `_fetched_at`. Re-runs of the same fire produce
        // identical (publish_ts, publish_slot) tuples and dedup
        // naturally; partitioning by publish-time keeps the file
        // boundary aligned with the publish-timeline question
        // consumers ask.
        self.publish_ts
    }
    fn dedup_key(&self) -> String {
        self.dedup_key()
    }
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        oracle_soothsayer_v6_band_tape::v2::to_record_batch(rows)
    }
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, FromArrowError> {
        oracle_soothsayer_v6_band_tape::v2::from_record_batch(batch)
    }
}
