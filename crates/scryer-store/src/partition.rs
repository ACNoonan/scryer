use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, Timelike, Utc};

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

/// `(year, month, day, hour)` in UTC, computed from a unix-second
/// timestamp.
///
/// Same role as [`UtcDay`] one granularity finer, for high-volume tapes
/// whose daily partition would exceed the ~500 MiB ceiling in the
/// "High-Volume Tape Partition Granularity" methodology entry. The
/// derived `Ord` is chronological for the same reason it is on `UtcDay`
/// — deterministic write order across partitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcHour {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
}

impl UtcHour {
    pub fn from_unix_seconds(ts: i64) -> Option<Self> {
        let dt = DateTime::<Utc>::from_timestamp(ts, 0)?;
        Some(Self {
            year: dt.year(),
            month: dt.month(),
            day: dt.day(),
            hour: dt.hour(),
        })
    }

    /// Hive-style relative path: `year=YYYY/month=MM/day=DD/hour=HH.parquet`.
    ///
    /// Note the `day=DD` segment is a directory here, where the daily
    /// layout makes it the filename. That difference is why a single
    /// `v{N}/` root may never mix the two — DuckDB rejects a mixed root
    /// with a hive-partition mismatch.
    pub fn relative_parquet_path(&self) -> PathBuf {
        PathBuf::from(format!(
            "year={:04}/month={:02}/day={:02}/hour={:02}.parquet",
            self.year, self.month, self.day, self.hour
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

/// Resolve the absolute partition file path for an hourly no-key
/// (event-stream) dataset, e.g. `solana.jito::bundle_tape` post-cutover:
/// `{venue}/{data_type}/v{N}/year=Y/month=M/day=D/hour=H.parquet`.
/// Locked by the "High-Volume Tape Partition Granularity" methodology
/// entry (2026-08-03).
pub fn partition_path_no_key_hourly(
    root: &Path,
    venue: &str,
    data_type: &str,
    schema_major: u32,
    hour: UtcHour,
) -> PathBuf {
    root.join(venue)
        .join(data_type)
        .join(format!("v{}", schema_major))
        .join(hour.relative_parquet_path())
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

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-06-30T23:59:59Z and the second that follows it.
    const LAST_SECOND_OF_JUNE: i64 = 1_782_863_999;
    const FIRST_SECOND_OF_JULY: i64 = 1_782_864_000;

    #[test]
    fn hourly_path_matches_methodology() {
        let hour = UtcHour::from_unix_seconds(LAST_SECOND_OF_JUNE).unwrap();
        let got =
            partition_path_no_key_hourly(Path::new("/root"), "solana.jito", "bundle_tape", 2, hour);
        assert_eq!(
            got,
            Path::new("/root/solana.jito/bundle_tape/v2/year=2026/month=06/day=30/hour=23.parquet")
        );
    }

    /// The hour bucket must carry the day with it. A rollover that kept
    /// the old day would silently file the first rows of a new day into
    /// the previous day's directory, where dedup would never see them.
    #[test]
    fn utc_hour_rolls_the_day_over_at_midnight() {
        let before = UtcHour::from_unix_seconds(LAST_SECOND_OF_JUNE).unwrap();
        let after = UtcHour::from_unix_seconds(FIRST_SECOND_OF_JULY).unwrap();

        assert_eq!(
            before,
            UtcHour {
                year: 2026,
                month: 6,
                day: 30,
                hour: 23
            }
        );
        assert_eq!(
            after,
            UtcHour {
                year: 2026,
                month: 7,
                day: 1,
                hour: 0
            }
        );
        // Derived `Ord` must stay chronological across the boundary —
        // `group_by_partition` relies on it for deterministic write order.
        assert!(before < after);
    }

    /// Every hour of a day must map to a distinct partition path, or the
    /// ~16x write-amplification reduction the hourly layout exists for
    /// silently collapses back toward daily.
    #[test]
    fn each_hour_of_a_day_gets_its_own_partition() {
        let midnight = 1_782_777_600; // 2026-06-30T00:00:00Z
        let paths: std::collections::BTreeSet<PathBuf> = (0..24)
            .map(|h| {
                let hour = UtcHour::from_unix_seconds(midnight + h * 3600).unwrap();
                partition_path_no_key_hourly(Path::new("/root"), "v", "dt", 2, hour)
            })
            .collect();
        assert_eq!(paths.len(), 24, "expected 24 distinct hourly partitions");
    }
}
