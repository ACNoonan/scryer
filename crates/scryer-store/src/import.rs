//! Import existing parquet files (e.g. `quant-work/data/*.parquet`)
//! into scryer's `dataset/` layout.
//!
//! The legacy parquet files were written by pandas/pyarrow before
//! scryer existed. They have the same logical column set as
//! `swap.v1` / `trade.v1` but no `_meta` columns. The functions here
//! synthesize `_schema_version` / `_fetched_at` / `_source` from
//! caller-supplied `ImportOptions`, so that imported rows are
//! indistinguishable from natively-written ones except by the
//! `_source` label they carry.

use std::fs::File;
use std::path::Path;
use std::time::SystemTime;

use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use scryer_schema::kamino_scope::v1 as kamino_scope_v1;
use scryer_schema::pyth::v1 as pyth_v1;
use scryer_schema::swap::v1 as swap_v1;
use scryer_schema::trade::v1 as trade_v1;
use scryer_schema::{FromArrowError, Meta};

use crate::error::StoreError;

#[derive(Clone, Debug)]
pub struct ImportOptions {
    /// Label stamped into `_source` on every imported row.
    pub source_label: String,
    /// Unix seconds stamped into `_fetched_at`. Default: file mtime.
    pub fetched_at: i64,
}

impl ImportOptions {
    pub fn from_file_mtime(
        path: &Path,
        source_label: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let mt = std::fs::metadata(path).map_err(|e| StoreError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let secs = mt
            .modified()
            .ok()
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        Ok(Self {
            source_label: source_label.into(),
            fetched_at: secs,
        })
    }
}

/// Read swap rows from an existing parquet file with the legacy
/// quant-work shape. Required columns: `signature`, `slot`, `ts`,
/// `side`, `sol_amount`, `usdc_amount`, `price`. Extra columns
/// (`dt`, `_meta` columns from a previous scryer run, etc.) are
/// ignored.
pub fn read_legacy_swap_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<swap_v1::Swap>, StoreError> {
    let file = File::open(path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        out.extend(extract_swaps(&batch, opts)?);
    }
    Ok(out)
}

/// Read Kamino Scope tape rows from an existing soothsayer
/// `kamino_scope_tape_YYYYMMDD.parquet`. Required columns: `poll_ts`,
/// `symbol`, `feed_pda`, `chain_id`, `scope_value_raw`, `scope_exp`,
/// `scope_price`, `scope_slot`, `scope_unix_ts`, `scope_age_s`. The
/// `scope_err` column is read if present (nullable) and tolerated as
/// either `LargeUtf8` (typical) or pyarrow's `null` dtype (when the
/// column is fully null in the source — in which case all rows get
/// `scope_err = None`).
pub fn read_legacy_kamino_scope_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<kamino_scope_v1::Reading>, StoreError> {
    let file = File::open(path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        out.extend(extract_kamino_scope(&batch, opts)?);
    }
    Ok(out)
}

/// Read Pyth Hermes tape rows from an existing soothsayer
/// `pyth_xstock_tape_YYYYMMDD.parquet`. Required columns: `poll_ts`,
/// `poll_unix`, `symbol`, `session`, `pyth_feed_id`, `pyth_price`,
/// `pyth_conf`, `pyth_expo`, `pyth_publish_time`, `pyth_age_s`,
/// `pyth_half_width_bps`, `pyth_ema_price`, `pyth_ema_conf`,
/// `pyth_ema_publish_time`, `pyth_ema_half_width_bps`, `slot`. The
/// `pyth_err` column is read if present (nullable) and tolerates
/// pyarrow's `null` dtype.
pub fn read_legacy_pyth_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<pyth_v1::Reading>, StoreError> {
    let file = File::open(path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        out.extend(extract_pyth(&batch, opts)?);
    }
    Ok(out)
}

/// Same as `read_legacy_swap_parquet` but for `trade.v1`. Required
/// columns: `price`, `volume`, `ts`, `side`, `type`, `misc`,
/// `trade_id`.
pub fn read_legacy_trade_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<trade_v1::Trade>, StoreError> {
    let file = File::open(path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        out.extend(extract_trades(&batch, opts)?);
    }
    Ok(out)
}

fn extract_swaps(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<swap_v1::Swap>, StoreError> {
    let signature = string_column(batch, "signature")?;
    let slot = downcast::<Int64Array>(batch, "slot")?;
    let ts = downcast::<Int64Array>(batch, "ts")?;
    let side = string_column(batch, "side")?;
    let sol_amount = downcast::<Float64Array>(batch, "sol_amount")?;
    let usdc_amount = downcast::<Float64Array>(batch, "usdc_amount")?;
    let price = downcast::<Float64Array>(batch, "price")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let s = side.value(i);
        let parsed_side = swap_v1::Side::parse(&s).ok_or_else(|| {
            StoreError::Schema(FromArrowError::UnknownEnumValue {
                column: "side",
                value: s.clone(),
            })
        })?;
        out.push(swap_v1::Swap {
            signature: signature.value(i),
            slot: slot.value(i) as u64,
            ts: ts.value(i),
            side: parsed_side,
            sol_amount: sol_amount.value(i),
            usdc_amount: usdc_amount.value(i),
            price: price.value(i),
            meta: Meta::new(swap_v1::SCHEMA_VERSION, opts.fetched_at, opts.source_label.clone()),
        });
    }
    Ok(out)
}

fn extract_trades(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<trade_v1::Trade>, StoreError> {
    let price = downcast::<Float64Array>(batch, "price")?;
    let volume = downcast::<Float64Array>(batch, "volume")?;
    let ts = downcast::<Float64Array>(batch, "ts")?;
    let side = string_column(batch, "side")?;
    let r#type = string_column(batch, "type")?;
    let misc = string_column(batch, "misc")?;
    let trade_id = downcast::<Int64Array>(batch, "trade_id")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(trade_v1::Trade {
            price: price.value(i),
            volume: volume.value(i),
            ts: ts.value(i),
            side: side.value(i),
            r#type: r#type.value(i),
            misc: misc.value(i),
            trade_id: trade_id.value(i),
            meta: Meta::new(
                trade_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

fn extract_kamino_scope(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<kamino_scope_v1::Reading>, StoreError> {
    let poll_ts = string_column(batch, "poll_ts")?;
    let symbol = string_column(batch, "symbol")?;
    let feed_pda = string_column(batch, "feed_pda")?;
    let chain_id = downcast::<Int64Array>(batch, "chain_id")?;
    let scope_value_raw = downcast::<Int64Array>(batch, "scope_value_raw")?;
    let scope_exp = downcast::<Int64Array>(batch, "scope_exp")?;
    let scope_price = downcast::<Float64Array>(batch, "scope_price")?;
    let scope_slot = downcast::<Int64Array>(batch, "scope_slot")?;
    let scope_unix_ts = downcast::<Int64Array>(batch, "scope_unix_ts")?;
    let scope_age_s = downcast::<Int64Array>(batch, "scope_age_s")?;
    // scope_err: present + nullable in scryer-format files, but the
    // legacy soothsayer files emit pyarrow `null` dtype when every row
    // is null (no string ever observed). Treat both cases as "all None".
    let err_col = batch
        .schema()
        .index_of("scope_err")
        .ok()
        .map(|idx| batch.column(idx).clone());
    let err_typed: Option<&LargeStringArray> = err_col
        .as_ref()
        .and_then(|c| c.as_any().downcast_ref::<LargeStringArray>());

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let scope_err = err_typed.and_then(|a| {
            if a.is_null(i) {
                None
            } else {
                Some(a.value(i).to_string())
            }
        });
        out.push(kamino_scope_v1::Reading {
            poll_ts: poll_ts.value(i),
            symbol: symbol.value(i),
            feed_pda: feed_pda.value(i),
            chain_id: chain_id.value(i),
            scope_value_raw: scope_value_raw.value(i),
            scope_exp: scope_exp.value(i),
            scope_price: scope_price.value(i),
            scope_slot: scope_slot.value(i),
            scope_unix_ts: scope_unix_ts.value(i),
            scope_age_s: scope_age_s.value(i),
            scope_err,
            meta: Meta::new(
                kamino_scope_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

fn extract_pyth(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<pyth_v1::Reading>, StoreError> {
    let poll_ts = string_column(batch, "poll_ts")?;
    let poll_unix = downcast::<Int64Array>(batch, "poll_unix")?;
    let symbol = string_column(batch, "symbol")?;
    let session = string_column(batch, "session")?;
    let pyth_feed_id = string_column(batch, "pyth_feed_id")?;
    let pyth_price = downcast::<Float64Array>(batch, "pyth_price")?;
    let pyth_conf = downcast::<Float64Array>(batch, "pyth_conf")?;
    let pyth_expo = downcast::<Int64Array>(batch, "pyth_expo")?;
    let pyth_publish_time = downcast::<Int64Array>(batch, "pyth_publish_time")?;
    let pyth_age_s = downcast::<Int64Array>(batch, "pyth_age_s")?;
    let pyth_half_width_bps = downcast::<Float64Array>(batch, "pyth_half_width_bps")?;
    let pyth_ema_price = downcast::<Float64Array>(batch, "pyth_ema_price")?;
    let pyth_ema_conf = downcast::<Float64Array>(batch, "pyth_ema_conf")?;
    let pyth_ema_publish_time = downcast::<Int64Array>(batch, "pyth_ema_publish_time")?;
    let pyth_ema_half_width_bps = downcast::<Float64Array>(batch, "pyth_ema_half_width_bps")?;
    let slot = downcast::<Int64Array>(batch, "slot")?;
    // pyth_err: same null-dtype tolerance as kamino_scope's scope_err.
    let err_col = batch
        .schema()
        .index_of("pyth_err")
        .ok()
        .map(|idx| batch.column(idx).clone());
    let err_typed: Option<&LargeStringArray> = err_col
        .as_ref()
        .and_then(|c| c.as_any().downcast_ref::<LargeStringArray>());

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let pyth_err = err_typed.and_then(|a| {
            if a.is_null(i) {
                None
            } else {
                Some(a.value(i).to_string())
            }
        });
        out.push(pyth_v1::Reading {
            poll_ts: poll_ts.value(i),
            poll_unix: poll_unix.value(i),
            symbol: symbol.value(i),
            session: session.value(i),
            pyth_feed_id: pyth_feed_id.value(i),
            pyth_price: pyth_price.value(i),
            pyth_conf: pyth_conf.value(i),
            pyth_expo: pyth_expo.value(i),
            pyth_publish_time: pyth_publish_time.value(i),
            pyth_age_s: pyth_age_s.value(i),
            pyth_half_width_bps: pyth_half_width_bps.value(i),
            pyth_ema_price: pyth_ema_price.value(i),
            pyth_ema_conf: pyth_ema_conf.value(i),
            pyth_ema_publish_time: pyth_ema_publish_time.value(i),
            pyth_ema_half_width_bps: pyth_ema_half_width_bps.value(i),
            slot: slot.value(i),
            pyth_err,
            meta: Meta::new(
                pyth_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

fn downcast<'a, A: arrow_array::Array + 'static>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a A, StoreError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn(name)))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or(StoreError::Schema(FromArrowError::WrongType {
            column: name,
            expected: std::any::type_name::<A>(),
        }))
}

/// Accept either `Utf8` or `LargeUtf8` for a string column. Pandas
/// defaults to `LargeUtf8`; in-memory polars and pyarrow with
/// `string_view` writers occasionally produce plain `Utf8`.
struct StrCol<'a> {
    inner: StrColInner<'a>,
}
enum StrColInner<'a> {
    Large(&'a LargeStringArray),
    Std(&'a StringArray),
}
impl<'a> StrCol<'a> {
    fn value(&self, i: usize) -> String {
        match self.inner {
            StrColInner::Large(a) => a.value(i).to_string(),
            StrColInner::Std(a) => a.value(i).to_string(),
        }
    }
}

fn string_column<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<StrCol<'a>, StoreError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn(name)))?;
    let col = batch.column(idx);
    if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(StrCol {
            inner: StrColInner::Large(a),
        });
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(StrCol {
            inner: StrColInner::Std(a),
        });
    }
    Err(StoreError::Schema(FromArrowError::WrongType {
        column: name,
        expected: "LargeUtf8 or Utf8",
    }))
}
