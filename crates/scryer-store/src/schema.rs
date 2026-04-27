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
use scryer_schema::{kamino_scope, pyth, swap, trade, v5_tape, FromArrowError};

pub trait DatasetSchema: Sized + Clone {
    /// Path segment between `{venue}/` and `v{N}/`.
    const DATA_TYPE: &'static str;
    /// Major schema version for the `v{N}/` partition segment.
    const SCHEMA_MAJOR: u32 = 1;
    /// `Some("pool")` for keyed schemas, `None` for event-stream.
    const PARTITION_KEY_PREFIX: Option<&'static str>;

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
