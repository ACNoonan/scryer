//! `scryer-store` — partition layout, parquet writer, dedup enforcement.
//!
//! The only crate that writes to `dataset/`. Owns the canonical layouts
//! - keyed: `dataset/{venue}/{data_type}/v{N}/{prefix}={key}/year=Y/month=M/day=D.parquet`
//! - event-stream: `dataset/{venue}/{data_type}/v{N}/year=Y/month=M/day=D.parquet`
//!
//! and enforces per-schema `_dedup_key` semantics at write time:
//! re-fetching an already-pulled window produces identical parquet
//! content modulo `_fetched_at` (which is preserved per-row from the
//! existing partition for any row whose `_dedup_key` is already there).
//!
//! Operational decisions — read-modify-write dedup, sort-by-`_dedup_key`,
//! atomic tempfile + rename, UTC-day partitioning — are locked in
//! `methodology_log.md`'s "Storage layer operational policy" section.
//!
//! # Generic API
//!
//! ```no_run
//! use scryer_store::{venue, Dataset};
//! use scryer_schema::swap::v1::Swap;
//!
//! let ds = Dataset::new("./dataset");
//! let rows: Vec<Swap> = vec![/* ... */];
//! // Keyed schema (swap.v1 expects pool=...):
//! let _ = ds.write::<Swap>(venue::SOLANA_RAYDIUM_V4, Some("POOL_ADDR"), &rows);
//!
//! use scryer_schema::pyth::v1::Reading as PythReading;
//! let pyth_rows: Vec<PythReading> = vec![/* ... */];
//! // Event-stream schema (pyth.v1 is no-key):
//! let _ = ds.write::<PythReading>(venue::PYTH, None, &pyth_rows);
//! ```

pub mod error;
pub mod import;
mod partition;
pub mod schema;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub use error::StoreError;
pub use partition::UtcDay;
pub use schema::{DatasetSchema, PartitionGranularity, PartitionTime};

/// Venue-string conventions for v0.1. Fetcher crates pass these to
/// `Dataset::write`; future venues add new constants here rather than
/// inventing their own at the call site.
pub mod venue {
    pub const SOLANA_RAYDIUM_V4: &str = "solana_raydium_v4";
    pub const KRAKEN: &str = "kraken";
    pub const KAMINO_SCOPE: &str = "kamino_scope";
    pub const PYTH: &str = "pyth";
    pub const REDSTONE: &str = "redstone";
    pub const YAHOO: &str = "yahoo";
    pub const BACKED: &str = "backed";
    /// Soothsayer experiment v5 (Chainlink + Jupiter joined tape).
    /// Per the methodology log "Soothsayer venue versioning" section,
    /// each soothsayer experiment iteration gets its own venue
    /// (`soothsayer_v5`, `soothsayer_v6`, ...) so iterations can run
    /// in parallel without colliding.
    pub const SOOTHSAYER_V5: &str = "soothsayer_v5";
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub partitions_written: usize,
    pub rows_added: usize,
    pub rows_deduped: usize,
}

pub struct Dataset {
    root: PathBuf,
}

impl Dataset {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Write rows of any [`DatasetSchema`] to per-day partitions under
    /// `{root}/{venue}/{S::DATA_TYPE}/v{S::SCHEMA_MAJOR}/...`. The
    /// `partition_key` argument is required if `S::PARTITION_KEY_PREFIX`
    /// is `Some(_)`, must be `None` otherwise; mismatches return
    /// [`StoreError::PartitionKeyMismatch`]. Same read-modify-write
    /// dedup semantics regardless of partition shape.
    pub fn write<S: DatasetSchema>(
        &self,
        venue: &str,
        partition_key: Option<&str>,
        rows: &[S],
    ) -> Result<WriteStats, StoreError> {
        validate_partition_key::<S>(partition_key)?;
        if rows.is_empty() {
            return Ok(WriteStats::default());
        }
        let by_partition = group_by_partition::<S, _>(rows, |r| r.ts_unix_seconds())?;
        let mut stats = WriteStats::default();
        for (pt, part_rows) in by_partition {
            let path = partition_path_for::<S>(&self.root, venue, partition_key, pt);
            let existing = read_partition::<S>(&path)?;
            let new_count = part_rows.len();
            let (merged, deduped) = merge_dedup(existing, part_rows, |r| r.dedup_key());
            let batch = S::to_record_batch(&merged)?;
            write_batch_atomic(&path, &batch)?;
            stats.partitions_written += 1;
            stats.rows_added += new_count - deduped;
            stats.rows_deduped += deduped;
        }
        Ok(stats)
    }

    /// Read all rows from a single partition file. Returns an empty
    /// vec if the file does not exist. Useful for consumers that want
    /// to load a specific partition in Rust (Python consumers should
    /// use pyarrow directly).
    ///
    /// `time`'s variant must match `S::PARTITION_GRANULARITY`. A
    /// `UtcDay` auto-converts via `From` for `Daily` schemas:
    /// `ds.read::<Swap>(venue, key, day.into())`.
    pub fn read<S: DatasetSchema>(
        &self,
        venue: &str,
        partition_key: Option<&str>,
        time: impl Into<PartitionTime>,
    ) -> Result<Vec<S>, StoreError> {
        validate_partition_key::<S>(partition_key)?;
        let pt = time.into();
        if pt.granularity() != S::PARTITION_GRANULARITY {
            return Err(StoreError::Arrow(arrow_schema::ArrowError::ComputeError(format!(
                "schema `{}` is {:?}-granular but read was passed {:?} time",
                std::any::type_name::<S>(),
                S::PARTITION_GRANULARITY,
                pt.granularity(),
            ))));
        }
        let path = partition_path_for::<S>(&self.root, venue, partition_key, pt);
        read_partition::<S>(&path)
    }
}

fn validate_partition_key<S: DatasetSchema>(provided: Option<&str>) -> Result<(), StoreError> {
    match (S::PARTITION_KEY_PREFIX, provided) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(StoreError::PartitionKeyMismatch {
            schema: std::any::type_name::<S>(),
            expected_prefix: S::PARTITION_KEY_PREFIX,
            provided_key: provided.is_some(),
        }),
    }
}

fn partition_path_for<S: DatasetSchema>(
    root: &Path,
    venue: &str,
    partition_key: Option<&str>,
    time: PartitionTime,
) -> PathBuf {
    match (S::PARTITION_KEY_PREFIX, partition_key, time) {
        (Some(prefix), Some(key), PartitionTime::Daily(day)) => partition::partition_path(
            root,
            venue,
            S::DATA_TYPE,
            S::SCHEMA_MAJOR,
            prefix,
            key,
            day,
        ),
        (Some(prefix), Some(key), PartitionTime::Yearly(year)) => {
            partition::partition_path_keyed_yearly(
                root,
                venue,
                S::DATA_TYPE,
                S::SCHEMA_MAJOR,
                prefix,
                key,
                year,
            )
        }
        (None, _, PartitionTime::Daily(day)) => {
            partition::partition_path_no_key(root, venue, S::DATA_TYPE, S::SCHEMA_MAJOR, day)
        }
        (None, _, PartitionTime::Yearly(year)) => {
            // No-key + Yearly: dataset/{venue}/{data_type}/v{N}/year=YYYY.parquet.
            // Not yet used but keeping the dispatch complete.
            root.join(venue)
                .join(S::DATA_TYPE)
                .join(format!("v{}", S::SCHEMA_MAJOR))
                .join(format!("year={:04}.parquet", year))
        }
        // Keyed schema called without a key — caught by validate_partition_key.
        (Some(_), None, _) => unreachable!("validate_partition_key ensures keyed schema has key"),
    }
}

fn read_partition<S: DatasetSchema>(path: &Path) -> Result<Vec<S>, StoreError> {
    let Some(reader) = open_parquet_reader(path)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(parquet::errors::ParquetError::from)?;
        out.extend(S::from_record_batch(&batch)?);
    }
    Ok(out)
}

/// Bucket rows by their partition time according to the schema's
/// `PARTITION_GRANULARITY`. Dispatches at compile time via the trait
/// const; per-row cost is one `chrono::DateTime` parse plus a
/// `BTreeMap` insert.
fn group_by_partition<S, F>(
    rows: &[S],
    get_ts: F,
) -> Result<BTreeMap<PartitionTime, Vec<S>>, StoreError>
where
    S: DatasetSchema,
    F: Fn(&S) -> i64,
{
    let mut by: BTreeMap<PartitionTime, Vec<S>> = BTreeMap::new();
    for r in rows {
        let ts = get_ts(r);
        let pt = match S::PARTITION_GRANULARITY {
            PartitionGranularity::Daily => {
                let day = UtcDay::from_unix_seconds(ts).ok_or_else(|| {
                    StoreError::Arrow(arrow_schema::ArrowError::ComputeError(format!(
                        "timestamp {ts} (unix seconds) out of representable range for UTC date"
                    )))
                })?;
                PartitionTime::Daily(day)
            }
            PartitionGranularity::Yearly => {
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0).ok_or_else(|| {
                    StoreError::Arrow(arrow_schema::ArrowError::ComputeError(format!(
                        "timestamp {ts} (unix seconds) out of representable range for year"
                    )))
                })?;
                use chrono::Datelike;
                PartitionTime::Yearly(dt.year())
            }
        };
        by.entry(pt).or_default().push(r.clone());
    }
    Ok(by)
}

/// Merge `new` rows into `existing` keyed by `_dedup_key`. Existing rows
/// win on collision — preserving their original `_fetched_at` /
/// `_source`. Returns the merged vec (sorted by `_dedup_key` ascending,
/// per the operational policy) and the count of new rows that were
/// dropped because their key already existed.
fn merge_dedup<T, F>(existing: Vec<T>, new: Vec<T>, key: F) -> (Vec<T>, usize)
where
    F: Fn(&T) -> String,
{
    let mut by_key: BTreeMap<String, T> = BTreeMap::new();
    for t in existing {
        let k = key(&t);
        by_key.insert(k, t);
    }
    let mut deduped = 0;
    for t in new {
        let k = key(&t);
        if by_key.contains_key(&k) {
            deduped += 1;
        } else {
            by_key.insert(k, t);
        }
    }
    (by_key.into_values().collect(), deduped)
}

fn open_parquet_reader(
    path: &Path,
) -> Result<Option<parquet::arrow::arrow_reader::ParquetRecordBatchReader>, StoreError> {
    if !path.exists() {
        return Ok(None);
    }
    let file = File::open(path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(Some(builder.build()?))
}

/// Atomic write: serialize `batch` to `{path}.tmp` (parquet, snappy),
/// fsync, then rename into place. A `scry` process killed between
/// `create` and `rename` leaves any prior version of `path` intact.
fn write_batch_atomic(path: &Path, batch: &RecordBatch) -> Result<(), StoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::Io {
            path: parent.to_path_buf(),
            source: e,
        })?;
    }
    let tmp_path = tmp_path_for(path);
    {
        let file = File::create(&tmp_path).map_err(|e| StoreError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
    }
    {
        let file = File::open(&tmp_path).map_err(|e| StoreError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
        file.sync_all().map_err(|e| StoreError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
    }
    std::fs::rename(&tmp_path, path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    Ok(())
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}
