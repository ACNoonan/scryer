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
    backed, earnings, kamino_scope, nasdaq_halts, pyth, redstone, swap, trade, v5_tape, yahoo,
    FromArrowError,
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
    /// `year=YYYY.parquet`. Used by daily-bar data (Yahoo OHLCV) and
    /// other low-frequency keyed datasets where ~250 rows/year would
    /// produce single-row files at daily granularity.
    Yearly,
}

/// Concrete time partition that maps directly to a partition path
/// segment. Daily for `year=Y/month=M/day=D.parquet` schemas; Yearly
/// for `year=Y.parquet` schemas.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PartitionTime {
    Daily(crate::partition::UtcDay),
    Yearly(i32),
}

impl PartitionTime {
    pub fn granularity(&self) -> PartitionGranularity {
        match self {
            Self::Daily(_) => PartitionGranularity::Daily,
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
