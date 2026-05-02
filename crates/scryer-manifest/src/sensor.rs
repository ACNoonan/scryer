//! Source-manifest workflow sensor parser.
//!
//! Sensor expressions live in the `[workflow]` block. The methodology
//! lock (`Source manifest format`, 2026-05-02) defines four kinds:
//!
//! - `interval(<secs>s)` — fire every N seconds.
//! - `daily(<HH:MM>Z)` — fire at the given UTC wall-clock minute.
//! - `backfill_complete(<schema_id>[, min_rows_per_day=N])` — fire
//!   once a backfill of `<schema_id>` lands a partition that clears
//!   the optional `min_rows_per_day` floor.
//! - `partitions_aged(<schema_id>, max_age_secs=N)` — fire when the
//!   newest partition for `<schema_id>` exceeds `max_age_secs`.
//!
//! This module parses the *shape* and a strict subset of the
//! semantics. Sensor *evaluation* is M3.2 (sensor primitives); the
//! parser exists so manifests with malformed sensors fail at
//! load-time instead of runner-startup.

use scryer_schema::{is_known_v1_schema, SchemaId};

use crate::error::ManifestError;

/// Parsed sensor expression. Variants carry the parsed arguments;
/// raw strings are preserved on the `Manifest` for diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sensor {
    Interval { secs: u64 },
    Daily { hour: u8, minute: u8 },
    BackfillComplete {
        schema_id: String,
        min_rows_per_day: Option<u64>,
    },
    PartitionsAged {
        schema_id: String,
        max_age_secs: u64,
    },
}

impl Sensor {
    pub fn parse(raw: &str) -> Result<Self, ManifestError> {
        let trimmed = raw.trim();
        let (head, body) = split_call(trimmed).ok_or_else(|| ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "expected `<kind>(<args>)`",
        })?;
        match head {
            "interval" => parse_interval(raw, body),
            "daily" => parse_daily(raw, body),
            "backfill_complete" => parse_backfill_complete(raw, body),
            "partitions_aged" => parse_partitions_aged(raw, body),
            other => Err(ManifestError::SensorUnknownKind {
                raw: raw.to_owned(),
                kind: other.to_owned(),
            }),
        }
    }
}

fn split_call(s: &str) -> Option<(&str, &str)> {
    let open = s.find('(')?;
    if !s.ends_with(')') {
        return None;
    }
    let head = &s[..open];
    let body = &s[open + 1..s.len() - 1];
    if head.is_empty() {
        return None;
    }
    Some((head, body))
}

fn parse_interval(raw: &str, body: &str) -> Result<Sensor, ManifestError> {
    let body = body.trim();
    let digits = body
        .strip_suffix('s')
        .ok_or_else(|| ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "interval body must end with `s`",
        })?;
    let secs: u64 = digits.parse().map_err(|_| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "interval seconds must be a non-negative integer",
    })?;
    if secs == 0 {
        return Err(ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "interval seconds must be >= 1",
        });
    }
    Ok(Sensor::Interval { secs })
}

fn parse_daily(raw: &str, body: &str) -> Result<Sensor, ManifestError> {
    let body = body.trim();
    let hhmm = body
        .strip_suffix('Z')
        .ok_or_else(|| ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "daily wall-clock must end with `Z` (UTC)",
        })?;
    let (h, m) = hhmm.split_once(':').ok_or_else(|| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "daily wall-clock must be `HH:MMZ`",
    })?;
    let hour: u8 = h.parse().map_err(|_| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "daily hour must be 00..=23",
    })?;
    let minute: u8 = m.parse().map_err(|_| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "daily minute must be 00..=59",
    })?;
    if hour > 23 {
        return Err(ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "daily hour must be 00..=23",
        });
    }
    if minute > 59 {
        return Err(ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "daily minute must be 00..=59",
        });
    }
    Ok(Sensor::Daily { hour, minute })
}

fn parse_backfill_complete(raw: &str, body: &str) -> Result<Sensor, ManifestError> {
    let parts = split_args(body);
    let mut iter = parts.into_iter();
    let schema_id_raw = iter.next().ok_or_else(|| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "backfill_complete requires a schema id as the first argument",
    })?;
    let schema_id = validate_schema_arg(raw, schema_id_raw)?;

    let mut min_rows_per_day: Option<u64> = None;
    for kv in iter {
        let (k, v) = kv.split_once('=').ok_or_else(|| ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "backfill_complete extra args must be `key=value`",
        })?;
        match k.trim() {
            "min_rows_per_day" => {
                let n: u64 = v.trim().parse().map_err(|_| ManifestError::SensorShape {
                    raw: raw.to_owned(),
                    reason: "min_rows_per_day must be a non-negative integer",
                })?;
                min_rows_per_day = Some(n);
            }
            _ => {
                return Err(ManifestError::SensorShape {
                    raw: raw.to_owned(),
                    reason: "backfill_complete only accepts `min_rows_per_day` today",
                });
            }
        }
    }
    Ok(Sensor::BackfillComplete {
        schema_id,
        min_rows_per_day,
    })
}

fn parse_partitions_aged(raw: &str, body: &str) -> Result<Sensor, ManifestError> {
    let parts = split_args(body);
    let mut iter = parts.into_iter();
    let schema_id_raw = iter.next().ok_or_else(|| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "partitions_aged requires a schema id as the first argument",
    })?;
    let schema_id = validate_schema_arg(raw, schema_id_raw)?;

    let mut max_age_secs: Option<u64> = None;
    for kv in iter {
        let (k, v) = kv.split_once('=').ok_or_else(|| ManifestError::SensorShape {
            raw: raw.to_owned(),
            reason: "partitions_aged extra args must be `key=value`",
        })?;
        match k.trim() {
            "max_age_secs" => {
                let n: u64 = v.trim().parse().map_err(|_| ManifestError::SensorShape {
                    raw: raw.to_owned(),
                    reason: "max_age_secs must be a non-negative integer",
                })?;
                if n == 0 {
                    return Err(ManifestError::SensorShape {
                        raw: raw.to_owned(),
                        reason: "max_age_secs must be >= 1",
                    });
                }
                max_age_secs = Some(n);
            }
            _ => {
                return Err(ManifestError::SensorShape {
                    raw: raw.to_owned(),
                    reason: "partitions_aged only accepts `max_age_secs` today",
                });
            }
        }
    }
    let max_age_secs = max_age_secs.ok_or_else(|| ManifestError::SensorShape {
        raw: raw.to_owned(),
        reason: "partitions_aged requires `max_age_secs=<n>`",
    })?;
    Ok(Sensor::PartitionsAged {
        schema_id,
        max_age_secs,
    })
}

fn split_args(body: &str) -> Vec<String> {
    body.split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

fn validate_schema_arg(raw: &str, arg: String) -> Result<String, ManifestError> {
    let trimmed = arg.trim().to_owned();
    if SchemaId::parse(&trimmed).is_ok() || is_known_v1_schema(&trimmed) {
        Ok(trimmed)
    } else {
        Err(ManifestError::SensorUnknownSchema {
            raw: raw.to_owned(),
            schema_id: trimmed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_interval() {
        assert_eq!(Sensor::parse("interval(3600s)").unwrap(), Sensor::Interval { secs: 3600 });
    }

    #[test]
    fn rejects_zero_interval() {
        assert!(matches!(
            Sensor::parse("interval(0s)"),
            Err(ManifestError::SensorShape { .. })
        ));
    }

    #[test]
    fn rejects_interval_without_s_suffix() {
        assert!(matches!(
            Sensor::parse("interval(3600)"),
            Err(ManifestError::SensorShape { .. })
        ));
    }

    #[test]
    fn parses_daily_utc() {
        assert_eq!(
            Sensor::parse("daily(13:30Z)").unwrap(),
            Sensor::Daily { hour: 13, minute: 30 }
        );
    }

    #[test]
    fn rejects_daily_out_of_range() {
        assert!(matches!(
            Sensor::parse("daily(24:00Z)"),
            Err(ManifestError::SensorShape { .. })
        ));
        assert!(matches!(
            Sensor::parse("daily(00:60Z)"),
            Err(ManifestError::SensorShape { .. })
        ));
    }

    #[test]
    fn rejects_daily_local_time() {
        assert!(matches!(
            Sensor::parse("daily(13:30)"),
            Err(ManifestError::SensorShape { .. })
        ));
    }

    #[test]
    fn parses_backfill_complete_v1() {
        let s = Sensor::parse("backfill_complete(trade.v1)").unwrap();
        match s {
            Sensor::BackfillComplete { schema_id, min_rows_per_day } => {
                assert_eq!(schema_id, "trade.v1");
                assert_eq!(min_rows_per_day, None);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parses_backfill_complete_v2_with_min_rows() {
        let s = Sensor::parse("backfill_complete(solana.kamino.liquidation.v2, min_rows_per_day=100)").unwrap();
        match s {
            Sensor::BackfillComplete { schema_id, min_rows_per_day } => {
                assert_eq!(schema_id, "solana.kamino.liquidation.v2");
                assert_eq!(min_rows_per_day, Some(100));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn rejects_backfill_complete_with_unknown_schema() {
        assert!(matches!(
            Sensor::parse("backfill_complete(not_a_schema.v1)"),
            Err(ManifestError::SensorUnknownSchema { .. })
        ));
    }

    #[test]
    fn parses_partitions_aged() {
        let s = Sensor::parse("partitions_aged(trade.v1, max_age_secs=86400)").unwrap();
        match s {
            Sensor::PartitionsAged { schema_id, max_age_secs } => {
                assert_eq!(schema_id, "trade.v1");
                assert_eq!(max_age_secs, 86400);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn rejects_partitions_aged_without_max_age() {
        assert!(matches!(
            Sensor::parse("partitions_aged(trade.v1)"),
            Err(ManifestError::SensorShape { .. })
        ));
    }

    #[test]
    fn rejects_unknown_sensor_kind() {
        assert!(matches!(
            Sensor::parse("on_demand()"),
            Err(ManifestError::SensorUnknownKind { .. })
        ));
    }

    #[test]
    fn rejects_missing_parens() {
        assert!(matches!(
            Sensor::parse("interval"),
            Err(ManifestError::SensorShape { .. })
        ));
    }
}
