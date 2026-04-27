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

use arrow_array::{
    Array, Date32Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use scryer_schema::backed::v1 as backed_v1;
use scryer_schema::earnings::v1 as earnings_v1;
use scryer_schema::kamino_scope::v1 as kamino_scope_v1;
use scryer_schema::pyth::v1 as pyth_v1;
use scryer_schema::redstone::v1 as redstone_v1;
use scryer_schema::swap::v1 as swap_v1;
use scryer_schema::trade::v1 as trade_v1;
use scryer_schema::v5_tape::v1 as v5_tape_v1;
use scryer_schema::yahoo::v1 as yahoo_v1;
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

/// Generic legacy-parquet reader. Each per-schema public reader is a
/// thin wrapper that supplies the schema-specific `extract` closure
/// (which knows the column names and types). The shared body owns
/// the file open / `ParquetRecordBatchReaderBuilder` boilerplate.
pub fn read_legacy_parquet<T, F>(
    path: &Path,
    opts: &ImportOptions,
    extract: F,
) -> Result<Vec<T>, StoreError>
where
    F: Fn(&RecordBatch, &ImportOptions) -> Result<Vec<T>, StoreError>,
{
    let file = File::open(path).map_err(|e| StoreError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        out.extend(extract(&batch, opts)?);
    }
    Ok(out)
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
    read_legacy_parquet(path, opts, extract_swaps)
}

/// Read Kamino Scope tape rows from an existing soothsayer
/// `kamino_scope_tape_YYYYMMDD.parquet`. Required columns: `poll_ts`,
/// `symbol`, `feed_pda`, `chain_id`, `scope_value_raw`, `scope_exp`,
/// `scope_price`, `scope_slot`, `scope_unix_ts`, `scope_age_s`. The
/// `scope_err` column is read if present (nullable) and tolerated as
/// either `LargeUtf8` (typical) or pyarrow's `null` dtype.
pub fn read_legacy_kamino_scope_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<kamino_scope_v1::Reading>, StoreError> {
    read_legacy_parquet(path, opts, extract_kamino_scope)
}

/// Read Pyth Hermes tape rows from an existing soothsayer
/// `pyth_xstock_tape_YYYYMMDD.parquet`. Same null-dtype tolerance
/// for `pyth_err` as kamino_scope's `scope_err`.
pub fn read_legacy_pyth_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<pyth_v1::Reading>, StoreError> {
    read_legacy_parquet(path, opts, extract_pyth)
}

/// Read Backed Finance corp-action commits from the existing
/// soothsayer `data/processed/backed_corp_actions.parquet`. The
/// `_enriched` derivative parquet is intentionally NOT supported
/// here — it's a soothsayer-side computed dataset, not raw upstream
/// data. Required columns: `detected_at`, `repo`, `commit_sha`,
/// `commit_date` (string `YYYY-MM-DD`, parsed to Date32 here),
/// `commit_url`, `title`, `underlying` (nullable),
/// `all_tickers_json`, `action_type`, `snippet`.
pub fn read_legacy_backed_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<backed_v1::Action>, StoreError> {
    read_legacy_parquet(path, opts, extract_backed)
}

/// Read earnings-calendar entries from one of the existing soothsayer
/// `data/raw/earnings_*.parquet` cache files. Required columns:
/// `symbol`, `earnings_date` (Date32).
pub fn read_legacy_earnings_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<earnings_v1::Event>, StoreError> {
    read_legacy_parquet(path, opts, extract_earnings)
}

/// Read Yahoo Finance OHLCV bars from one of the existing soothsayer
/// `data/raw/yahoo_*.parquet` cache files. Required columns:
/// `symbol`, `ts` (Date32), `open`, `high`, `low`, `close`,
/// `adj_close`, `volume`.
pub fn read_legacy_yahoo_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<yahoo_v1::Bar>, StoreError> {
    read_legacy_parquet(path, opts, extract_yahoo)
}

/// Read RedStone Live tape rows from the existing soothsayer
/// `redstone_live_tape.parquet` (single rolling file). Required
/// columns: `poll_ts`, `poll_label`, `symbol`, `redstone_ts`,
/// `minutes_age`, `value`, `provider_pubkey`, `signature`,
/// `source_json`, `permaweb_tx`, `raw_json`. The two timestamp
/// columns are arrow `Timestamp(Microsecond, UTC)` in the source
/// and stay that way in the scryer output.
pub fn read_legacy_redstone_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<redstone_v1::Reading>, StoreError> {
    read_legacy_parquet(path, opts, extract_redstone)
}

/// Read V5 tape rows from an existing soothsayer
/// `v5_tape_YYYYMMDD.parquet`. The Chainlink half (`cl_*` columns)
/// and `basis_bp` are nullable in the schema and tolerated as
/// pyarrow's `null` dtype in legacy files.
pub fn read_legacy_v5_tape_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<v5_tape_v1::Reading>, StoreError> {
    read_legacy_parquet(path, opts, extract_v5_tape)
}

/// Same as `read_legacy_swap_parquet` but for `trade.v1`. Required
/// columns: `price`, `volume`, `ts`, `side`, `type`, `misc`,
/// `trade_id`.
pub fn read_legacy_trade_parquet(
    path: &Path,
    opts: &ImportOptions,
) -> Result<Vec<trade_v1::Trade>, StoreError> {
    read_legacy_parquet(path, opts, extract_trades)
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

fn extract_backed(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<backed_v1::Action>, StoreError> {
    let detected_at = downcast::<TimestampMicrosecondArray>(batch, "detected_at")?;
    let repo = string_column(batch, "repo")?;
    let commit_sha = string_column(batch, "commit_sha")?;
    let commit_date_str = string_column(batch, "commit_date")?;
    let commit_url = string_column(batch, "commit_url")?;
    let title = string_column(batch, "title")?;
    let all_tickers_json = string_column(batch, "all_tickers_json")?;
    let action_type = string_column(batch, "action_type")?;
    let snippet = string_column(batch, "snippet")?;
    let underlying = optional_string_column(batch, "underlying")?;

    // Date32 epoch = 1970-01-01.
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("static date");

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let date_s = commit_date_str.value(i);
        let parsed = chrono::NaiveDate::parse_from_str(&date_s, "%Y-%m-%d").map_err(|_| {
            StoreError::Arrow(arrow_schema::ArrowError::ComputeError(format!(
                "could not parse commit_date `{date_s}` as YYYY-MM-DD"
            )))
        })?;
        let commit_date = parsed.signed_duration_since(epoch).num_days() as i32;
        out.push(backed_v1::Action {
            detected_at: detected_at.value(i),
            repo: repo.value(i),
            commit_sha: commit_sha.value(i),
            commit_date,
            commit_url: commit_url.value(i),
            title: title.value(i),
            underlying: underlying.value(i),
            all_tickers_json: all_tickers_json.value(i),
            action_type: action_type.value(i),
            snippet: snippet.value(i),
            meta: Meta::new(
                backed_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

fn extract_earnings(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<earnings_v1::Event>, StoreError> {
    let symbol = string_column(batch, "symbol")?;
    let earnings_date = downcast::<Date32Array>(batch, "earnings_date")?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(earnings_v1::Event {
            symbol: symbol.value(i),
            earnings_date: earnings_date.value(i),
            meta: Meta::new(
                earnings_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

/// `volume` in yfinance parquet is usually Int64 but occasionally
/// Float64 (e.g. futures contracts with fractional reporting).
/// Accept both; fractional values truncate to i64.
enum VolumeCol<'a> {
    Int(&'a Int64Array),
    Float(&'a Float64Array),
}
impl VolumeCol<'_> {
    fn value(&self, i: usize) -> i64 {
        match self {
            Self::Int(a) => a.value(i),
            Self::Float(a) => a.value(i) as i64,
        }
    }
}

/// `ts` in yfinance parquet is usually Date32 (days since epoch)
/// but occasionally TimestampMillisecond (when the call returned
/// intraday-precision data that pandas later coerced to a UTC
/// timestamp). Accept both; timestamps truncate to UTC calendar
/// day at the day-since-epoch level.
enum TsCol<'a> {
    Date(&'a Date32Array),
    TsMs(&'a TimestampMillisecondArray),
    TsUs(&'a TimestampMicrosecondArray),
}
impl TsCol<'_> {
    fn days_since_epoch(&self, i: usize) -> i32 {
        match self {
            Self::Date(a) => a.value(i),
            Self::TsMs(a) => (a.value(i) / 86_400_000) as i32,
            Self::TsUs(a) => (a.value(i) / 86_400_000_000) as i32,
        }
    }
}

fn extract_yahoo(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<yahoo_v1::Bar>, StoreError> {
    let symbol = string_column(batch, "symbol")?;
    let ts_idx = batch
        .schema()
        .index_of("ts")
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn("ts")))?;
    let ts_col = batch.column(ts_idx);
    let ts = if let Some(a) = ts_col.as_any().downcast_ref::<Date32Array>() {
        TsCol::Date(a)
    } else if let Some(a) = ts_col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        TsCol::TsMs(a)
    } else if let Some(a) = ts_col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        TsCol::TsUs(a)
    } else {
        return Err(StoreError::Schema(FromArrowError::WrongType {
            column: "ts",
            expected: "Date32 or Timestamp(Millisecond|Microsecond)",
        }));
    };
    let open = downcast::<Float64Array>(batch, "open")?;
    let high = downcast::<Float64Array>(batch, "high")?;
    let low = downcast::<Float64Array>(batch, "low")?;
    let close = downcast::<Float64Array>(batch, "close")?;
    let adj_close = downcast::<Float64Array>(batch, "adj_close")?;

    let volume_idx = batch
        .schema()
        .index_of("volume")
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn("volume")))?;
    let volume_col = batch.column(volume_idx);
    let volume = if let Some(a) = volume_col.as_any().downcast_ref::<Int64Array>() {
        VolumeCol::Int(a)
    } else if let Some(a) = volume_col.as_any().downcast_ref::<Float64Array>() {
        VolumeCol::Float(a)
    } else {
        return Err(StoreError::Schema(FromArrowError::WrongType {
            column: "volume",
            expected: "Int64 or Float64",
        }));
    };

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(yahoo_v1::Bar {
            symbol: symbol.value(i),
            ts: ts.days_since_epoch(i),
            open: open.value(i),
            high: high.value(i),
            low: low.value(i),
            close: close.value(i),
            adj_close: adj_close.value(i),
            volume: volume.value(i),
            meta: Meta::new(
                yahoo_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

fn extract_redstone(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<redstone_v1::Reading>, StoreError> {
    let poll_ts = downcast::<TimestampMicrosecondArray>(batch, "poll_ts")?;
    let poll_label = string_column(batch, "poll_label")?;
    let symbol = string_column(batch, "symbol")?;
    let redstone_ts = downcast::<TimestampMicrosecondArray>(batch, "redstone_ts")?;
    let minutes_age = downcast::<Int64Array>(batch, "minutes_age")?;
    let value = downcast::<Float64Array>(batch, "value")?;
    let provider_pubkey = string_column(batch, "provider_pubkey")?;
    let signature = string_column(batch, "signature")?;
    let source_json = string_column(batch, "source_json")?;
    let permaweb_tx = string_column(batch, "permaweb_tx")?;
    let raw_json = string_column(batch, "raw_json")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(redstone_v1::Reading {
            poll_ts: poll_ts.value(i),
            poll_label: poll_label.value(i),
            symbol: symbol.value(i),
            redstone_ts: redstone_ts.value(i),
            minutes_age: minutes_age.value(i),
            value: value.value(i),
            provider_pubkey: provider_pubkey.value(i),
            signature: signature.value(i),
            source_json: source_json.value(i),
            permaweb_tx: permaweb_tx.value(i),
            raw_json: raw_json.value(i),
            meta: Meta::new(
                redstone_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

fn extract_v5_tape(
    batch: &RecordBatch,
    opts: &ImportOptions,
) -> Result<Vec<v5_tape_v1::Reading>, StoreError> {
    let poll_ts = downcast::<Int64Array>(batch, "poll_ts")?;
    let symbol = string_column(batch, "symbol")?;
    let cl_obs_ts = optional_int64_column(batch, "cl_obs_ts")?;
    let cl_age_s = optional_int64_column(batch, "cl_age_s")?;
    let cl_tokenized_px = optional_float64_column(batch, "cl_tokenized_px")?;
    let cl_venue_px = optional_float64_column(batch, "cl_venue_px")?;
    let cl_market_status = optional_string_column(batch, "cl_market_status")?;
    let cl_err = string_column(batch, "cl_err")?;
    let jup_bid = downcast::<Float64Array>(batch, "jup_bid")?;
    let jup_ask = downcast::<Float64Array>(batch, "jup_ask")?;
    let jup_mid = downcast::<Float64Array>(batch, "jup_mid")?;
    let spread_bp = downcast::<Float64Array>(batch, "spread_bp")?;
    let jup_err = string_column(batch, "jup_err")?;
    let basis_bp = optional_float64_column(batch, "basis_bp")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(v5_tape_v1::Reading {
            poll_ts: poll_ts.value(i),
            symbol: symbol.value(i),
            cl_obs_ts: cl_obs_ts.value(i),
            cl_age_s: cl_age_s.value(i),
            cl_tokenized_px: cl_tokenized_px.value(i),
            cl_venue_px: cl_venue_px.value(i),
            cl_market_status: cl_market_status.value(i),
            cl_err: cl_err.value(i),
            jup_bid: jup_bid.value(i),
            jup_ask: jup_ask.value(i),
            jup_mid: jup_mid.value(i),
            spread_bp: spread_bp.value(i),
            jup_err: jup_err.value(i),
            basis_bp: basis_bp.value(i),
            meta: Meta::new(
                v5_tape_v1::SCHEMA_VERSION,
                opts.fetched_at,
                opts.source_label.clone(),
            ),
        });
    }
    Ok(out)
}

/// Optional-column accessors that tolerate pyarrow's `null` dtype.
/// Pandas writes a column with `null` dtype when every value in it
/// is null (e.g. v5_tape's `cl_*` columns when the US market was
/// closed for the entire file's window). The expected typed array
/// is also accepted, with per-row nullability handled by `is_null`.

enum OptionalInt64<'a> {
    Typed(&'a Int64Array),
    AllNull,
}
impl<'a> OptionalInt64<'a> {
    fn value(&self, i: usize) -> Option<i64> {
        match self {
            Self::Typed(a) if !a.is_null(i) => Some(a.value(i)),
            _ => None,
        }
    }
}

enum OptionalFloat64<'a> {
    Typed(&'a Float64Array),
    AllNull,
}
impl<'a> OptionalFloat64<'a> {
    fn value(&self, i: usize) -> Option<f64> {
        match self {
            Self::Typed(a) if !a.is_null(i) => Some(a.value(i)),
            _ => None,
        }
    }
}

enum OptionalStr<'a> {
    Large(&'a LargeStringArray),
    Std(&'a StringArray),
    AllNull,
}
impl<'a> OptionalStr<'a> {
    fn value(&self, i: usize) -> Option<String> {
        match self {
            Self::Large(a) if !a.is_null(i) => Some(a.value(i).to_string()),
            Self::Std(a) if !a.is_null(i) => Some(a.value(i).to_string()),
            _ => None,
        }
    }
}

fn optional_int64_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<OptionalInt64<'a>, StoreError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn(name)))?;
    let col = batch.column(idx);
    if col.data_type() == &arrow_schema::DataType::Null {
        return Ok(OptionalInt64::AllNull);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(OptionalInt64::Typed(a));
    }
    Err(StoreError::Schema(FromArrowError::WrongType {
        column: name,
        expected: "Int64 or Null",
    }))
}

fn optional_float64_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<OptionalFloat64<'a>, StoreError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn(name)))?;
    let col = batch.column(idx);
    if col.data_type() == &arrow_schema::DataType::Null {
        return Ok(OptionalFloat64::AllNull);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(OptionalFloat64::Typed(a));
    }
    Err(StoreError::Schema(FromArrowError::WrongType {
        column: name,
        expected: "Float64 or Null",
    }))
}

fn optional_string_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<OptionalStr<'a>, StoreError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| StoreError::Schema(FromArrowError::MissingColumn(name)))?;
    let col = batch.column(idx);
    if col.data_type() == &arrow_schema::DataType::Null {
        return Ok(OptionalStr::AllNull);
    }
    if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(OptionalStr::Large(a));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(OptionalStr::Std(a));
    }
    Err(StoreError::Schema(FromArrowError::WrongType {
        column: name,
        expected: "LargeUtf8 or Utf8 or Null",
    }))
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
