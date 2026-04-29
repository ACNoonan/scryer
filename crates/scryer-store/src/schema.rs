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
    backed, cboe_indices, cex_perp_funding_multi, cex_stock_perp_ohlcv, cex_stock_perp_tape, cme_intraday_1m, deribit_iv, dex_xstock_swaps, drift_liquidation, earnings, edgar_8k, evm_liquidation, fluid_vault_config, fred_macro, fred_macro_extended, geckoterminal, geckoterminal_ohlcv,
    jito_bundles, jito_tip_floor, jupiter_lend_liquidation, kamino_liquidation, kamino_obligation,
    kamino_obligation_position, kamino_reserve, kamino_scope, kraken_funding, loopscale_loan,
    loopscale_loan_collateral, mango_v4_liquidation, mango_v4_oracle_config, nasdaq_halts,
    oracle_context, pool_snapshot, pyth, pyth_poster_post, pyth_publisher,
    raydium_pool_metadata, redstone, solana_priority_fees, swap, trade, v5_tape, xstock_holders, yahoo, FromArrowError,
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
