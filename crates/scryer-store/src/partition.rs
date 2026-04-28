use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, Utc};

/// `(year, month, day)` in UTC, computed from a unix-second timestamp.
///
/// Used as the BTreeMap key when grouping rows into per-day partitions,
/// so the natural `Ord` here gives chronological partition order
/// (relevant for deterministic write order, even though final partition
/// content is independently determined by `_dedup_key` sort).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcDay {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

impl UtcDay {
    pub fn from_unix_seconds(ts: i64) -> Option<Self> {
        let dt = DateTime::<Utc>::from_timestamp(ts, 0)?;
        Some(Self {
            year: dt.year(),
            month: dt.month(),
            day: dt.day(),
        })
    }

    /// Hive-style relative path: `year=YYYY/month=MM/day=DD.parquet`.
    pub fn relative_parquet_path(&self) -> PathBuf {
        PathBuf::from(format!(
            "year={:04}/month={:02}/day={:02}.parquet",
            self.year, self.month, self.day
        ))
    }
}

/// Resolve the absolute partition file path for a (venue, data_type,
/// version, key_prefix=key_value, day) tuple. Per the
/// "Storage layer operational policy" methodology section, partition
/// path values are written literally (no URL encoding); v0.1 keys
/// (Solana base58, Kraken pair codes) contain no path-unsafe chars.
pub fn partition_path(
    root: &Path,
    venue: &str,
    data_type: &str,
    schema_major: u32,
    key_prefix: &str,
    key_value: &str,
    day: UtcDay,
) -> PathBuf {
    root.join(venue)
        .join(data_type)
        .join(format!("v{}", schema_major))
        .join(format!("{}={}", key_prefix, key_value))
        .join(day.relative_parquet_path())
}

/// Resolve the absolute partition file path for a no-key (event-stream)
/// dataset like `kamino_scope::oracle_tape`. Layout matches the
/// methodology log's "For event-stream data" form:
/// `{venue}/{data_type}/v{N}/year=Y/month=M/day=D.parquet`.
pub fn partition_path_no_key(
    root: &Path,
    venue: &str,
    data_type: &str,
    schema_major: u32,
    day: UtcDay,
) -> PathBuf {
    root.join(venue)
        .join(data_type)
        .join(format!("v{}", schema_major))
        .join(day.relative_parquet_path())
}

/// Resolve the absolute partition file path for a yearly-keyed
/// dataset (Phase 11+, e.g. Yahoo OHLCV bars). Layout matches the
/// methodology log's "For low-frequency keyed data" form:
/// `{venue}/{data_type}/v{N}/{prefix}={value}/year=YYYY.parquet`.
pub fn partition_path_keyed_yearly(
    root: &Path,
    venue: &str,
    data_type: &str,
    schema_major: u32,
    key_prefix: &str,
    key_value: &str,
    year: i32,
) -> PathBuf {
    root.join(venue)
        .join(data_type)
        .join(format!("v{}", schema_major))
        .join(format!("{}={}", key_prefix, key_value))
        .join(format!("year={:04}.parquet", year))
}

/// Resolve the absolute partition file path for a monthly-keyed
/// dataset (Phase 15+, e.g. Kraken Pro Futures funding rates).
/// Layout matches the methodology log's "monthly-keyed periodic
/// data" form:
/// `{venue}/{data_type}/v{N}/{prefix}={value}/year=YYYY/month=MM.parquet`.
pub fn partition_path_keyed_monthly(
    root: &Path,
    venue: &str,
    data_type: &str,
    schema_major: u32,
    key_prefix: &str,
    key_value: &str,
    year: i32,
    month: u32,
) -> PathBuf {
    root.join(venue)
        .join(data_type)
        .join(format!("v{}", schema_major))
        .join(format!("{}={}", key_prefix, key_value))
        .join(format!("year={:04}", year))
        .join(format!("month={:02}.parquet", month))
}

/// No-key + Monthly. Reserved for future schemas; not yet used.
pub fn partition_path_no_key_monthly(
    root: &Path,
    venue: &str,
    data_type: &str,
    schema_major: u32,
    year: i32,
    month: u32,
) -> PathBuf {
    root.join(venue)
        .join(data_type)
        .join(format!("v{}", schema_major))
        .join(format!("year={:04}", year))
        .join(format!("month={:02}.parquet", month))
}
