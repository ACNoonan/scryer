//! `scryer-store` — partition layout, parquet writer, dedup enforcement.
//!
//! The only crate that writes to `dataset/`. Owns the canonical layout
//! `dataset/{venue}/{data_type}/v{N}/{key_prefix}={key}/year=Y/month=M/day=D.parquet`
//! and enforces per-schema `_dedup_key` semantics at write time:
//! re-fetching an already-pulled window produces identical parquet
//! content modulo `_fetched_at` (which is preserved per-row from the
//! existing partition for any row whose `_dedup_key` is already there).
//!
//! Operational decisions — read-modify-write dedup, sort-by-`_dedup_key`,
//! atomic tempfile + rename, UTC-day partitioning — are locked in
//! `methodology_log.md`'s "Storage layer operational policy" section.

pub mod error;
pub mod import;
mod partition;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use scryer_schema::{swap, trade};

pub use error::StoreError;
pub use partition::UtcDay;

/// Venue-string conventions for v0.1. Fetcher crates pass these to
/// `Dataset::write_*`; future venues add new constants here rather than
/// inventing their own at the call site.
pub mod venue {
    pub const SOLANA_RAYDIUM_V4: &str = "solana_raydium_v4";
    pub const KRAKEN: &str = "kraken";
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

    /// Write swaps to per-day partitions under
    /// `{root}/{venue}/swaps/v1/pool={pool}/year=Y/month=M/day=D.parquet`.
    /// Read-modify-write semantics: existing rows with the same
    /// `_dedup_key` win on collision.
    pub fn write_swaps(
        &self,
        venue: &str,
        pool: &str,
        rows: &[swap::v1::Swap],
    ) -> Result<WriteStats, StoreError> {
        if rows.is_empty() {
            return Ok(WriteStats::default());
        }
        let by_day = group_by_day(rows, |r| r.ts)?;
        let mut stats = WriteStats::default();
        for (day, day_rows) in by_day {
            let path = partition::partition_path(
                &self.root, venue, "swaps", 1, "pool", pool, day,
            );
            let existing = read_swap_partition(&path)?;
            let new_count = day_rows.len();
            let (merged, deduped) = merge_dedup(existing, day_rows, |s| s.dedup_key());
            let batch = swap::v1::to_record_batch(&merged)?;
            write_batch_atomic(&path, &batch)?;
            stats.partitions_written += 1;
            stats.rows_added += new_count - deduped;
            stats.rows_deduped += deduped;
        }
        Ok(stats)
    }

    /// Write trades to per-day partitions under
    /// `{root}/{venue}/trades/v1/pair={pair}/year=Y/month=M/day=D.parquet`.
    /// Same read-modify-write semantics as `write_swaps`.
    pub fn write_trades(
        &self,
        venue: &str,
        pair: &str,
        rows: &[trade::v1::Trade],
    ) -> Result<WriteStats, StoreError> {
        if rows.is_empty() {
            return Ok(WriteStats::default());
        }
        let by_day = group_by_day(rows, |r| r.ts as i64)?;
        let mut stats = WriteStats::default();
        for (day, day_rows) in by_day {
            let path = partition::partition_path(
                &self.root, venue, "trades", 1, "pair", pair, day,
            );
            let existing = read_trade_partition(&path)?;
            let new_count = day_rows.len();
            let (merged, deduped) = merge_dedup(existing, day_rows, |t| t.dedup_key());
            let batch = trade::v1::to_record_batch(&merged)?;
            write_batch_atomic(&path, &batch)?;
            stats.partitions_written += 1;
            stats.rows_added += new_count - deduped;
            stats.rows_deduped += deduped;
        }
        Ok(stats)
    }

    /// Read all swap rows from a single partition file. Returns an empty
    /// vec if the file does not exist. Useful for consumers that want to
    /// load a specific day in Rust (Python consumers should use pyarrow
    /// directly).
    pub fn read_swaps(
        &self,
        venue: &str,
        pool: &str,
        day: UtcDay,
    ) -> Result<Vec<swap::v1::Swap>, StoreError> {
        let path = partition::partition_path(
            &self.root, venue, "swaps", 1, "pool", pool, day,
        );
        read_swap_partition(&path)
    }

    pub fn read_trades(
        &self,
        venue: &str,
        pair: &str,
        day: UtcDay,
    ) -> Result<Vec<trade::v1::Trade>, StoreError> {
        let path = partition::partition_path(
            &self.root, venue, "trades", 1, "pair", pair, day,
        );
        read_trade_partition(&path)
    }
}

fn group_by_day<T, F>(rows: &[T], get_ts: F) -> Result<BTreeMap<UtcDay, Vec<T>>, StoreError>
where
    T: Clone,
    F: Fn(&T) -> i64,
{
    let mut by_day: BTreeMap<UtcDay, Vec<T>> = BTreeMap::new();
    for r in rows {
        let ts = get_ts(r);
        let day = UtcDay::from_unix_seconds(ts).ok_or_else(|| {
            StoreError::Arrow(arrow_schema::ArrowError::ComputeError(format!(
                "timestamp {ts} (unix seconds) out of representable range for UTC date"
            )))
        })?;
        by_day.entry(day).or_default().push(r.clone());
    }
    Ok(by_day)
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

fn read_swap_partition(path: &Path) -> Result<Vec<swap::v1::Swap>, StoreError> {
    let Some(reader) = open_parquet_reader(path)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(parquet::errors::ParquetError::from)?;
        out.extend(swap::v1::from_record_batch(&batch)?);
    }
    Ok(out)
}

fn read_trade_partition(path: &Path) -> Result<Vec<trade::v1::Trade>, StoreError> {
    let Some(reader) = open_parquet_reader(path)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(parquet::errors::ParquetError::from)?;
        out.extend(trade::v1::from_record_batch(&batch)?);
    }
    Ok(out)
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
