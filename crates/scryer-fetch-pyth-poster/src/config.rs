//! Feed-allowlist + cadence configuration.
//!
//! Per the methodology lock ("Write-side daemon schemas — 2026-04-28
//! (locked)" §`pyth_poster_post.v1`):
//!
//! - **Pilot:** SPY only at v0 launch. Default config carries SPY only.
//! - **Closed list at v0.1:** SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD,
//!   MSTR, GLD, TLT. Anything outside this list requires a methodology
//!   entry before being added to the daemon's runtime config.
//! - **Cadence (locked defaults):**
//!   - `open_hours_cadence_secs: 60` — NYSE regular-hours.
//!   - `closed_hours_cadence_secs: 900` — weekday off-hours; null = skip.
//!   - `weekend_cadence_secs: null` — skip.
//!   - `skip_if_similar_bps: 5`.
//!   - `staleness_skip_threshold_secs: 300`.
//!
//! Override of these defaults requires a Decision-log row.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// All v0.1-permitted underlier tickers. Any feed-config that mentions
/// a ticker outside this set is rejected at load time so a config-file
/// edit alone can't silently expand coverage. Methodology-entry first,
/// allowlist update after.
pub const V0_1_PERMITTED_UNDERLIERS: &[&str] = &[
    "SPY", "QQQ", "AAPL", "GOOGL", "NVDA", "TSLA", "HOOD", "MSTR", "GLD", "TLT",
];

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config parse error: {0}")]
    Parse(String),

    #[error(
        "ticker `{ticker}` is not in the v0.1 methodology allowlist {permitted:?} — \
         add a methodology-log entry first per `methodology_log.md` \
         'Write-side daemon schemas' § feed-allowlist policy"
    )]
    TickerNotPermitted {
        ticker: String,
        permitted: &'static [&'static str],
    },

    #[error("duplicate underlier `{0}` in feed config")]
    DuplicateUnderlier(String),

    #[error("invalid cadence: {0}")]
    InvalidCadence(String),
}

/// Per-feed runtime configuration: a feed-id and the underlier ticker
/// it's mapped to. Cadence is uniform across feeds (controlled by
/// `FeedDefaults`) — per-feed cadence overrides aren't part of v0.1
/// scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedConfig {
    /// 32-byte feed id, hex, lowercase, no `0x` prefix.
    pub feed_id_hex: String,
    /// Resolved ticker — must be in `V0_1_PERMITTED_UNDERLIERS`.
    pub underlier_symbol: String,
}

/// Daemon-wide cadence + skip-policy defaults. Locked values per the
/// methodology entry; this struct documents each as a separate field
/// so a non-default override is visible at startup.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FeedDefaults {
    /// NYSE regular-hours cadence (seconds). Methodology default: 60.
    pub open_hours_cadence_secs: u32,
    /// Weekday off-hours cadence. `None` = skip. Methodology default: 900.
    pub closed_hours_cadence_secs: Option<u32>,
    /// Weekend cadence. `None` = skip. Methodology default: None.
    pub weekend_cadence_secs: Option<u32>,
    /// Skip post if Hermes price is within N bps of on-chain price AND
    /// on-chain publish_time is within `staleness_skip_threshold_secs`.
    /// Methodology default: 5.
    pub skip_if_similar_bps: u32,
    /// On-chain `publish_time` staleness threshold for the skip-if-
    /// similar gate. Methodology default: 300.
    pub staleness_skip_threshold_secs: u32,
}

impl Default for FeedDefaults {
    fn default() -> Self {
        Self {
            open_hours_cadence_secs: 60,
            closed_hours_cadence_secs: Some(900),
            weekend_cadence_secs: None,
            skip_if_similar_bps: 5,
            staleness_skip_threshold_secs: 300,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PosterConfig {
    pub defaults: FeedDefaults,
    pub feeds: Vec<FeedConfig>,
}

impl PosterConfig {
    /// Pilot config: SPY only, no feed_id resolved yet (filled by
    /// boot-time discovery against Hermes if the operator hasn't
    /// pre-seeded the file).
    pub fn pilot_spy_placeholder() -> Self {
        Self {
            defaults: FeedDefaults::default(),
            feeds: vec![FeedConfig {
                feed_id_hex: String::new(),
                underlier_symbol: "SPY".to_string(),
            }],
        }
    }

    /// Default config-file path:
    /// `~/Library/Application Support/scryer/config/pyth_poster_feeds.toml`.
    pub fn default_path() -> PathBuf {
        if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
            home.join("Library")
                .join("Application Support")
                .join("scryer")
                .join("config")
                .join("pyth_poster_feeds.toml")
        } else {
            PathBuf::from("./pyth_poster_feeds.toml")
        }
    }

    /// Validate config against the v0.1 methodology rules:
    /// - every ticker is in `V0_1_PERMITTED_UNDERLIERS`
    /// - no duplicate underliers
    /// - cadence values are coherent
    pub fn validate(&self) -> Result<(), ConfigError> {
        let permitted: BTreeSet<&str> = V0_1_PERMITTED_UNDERLIERS.iter().copied().collect();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for f in &self.feeds {
            if !permitted.contains(f.underlier_symbol.as_str()) {
                return Err(ConfigError::TickerNotPermitted {
                    ticker: f.underlier_symbol.clone(),
                    permitted: V0_1_PERMITTED_UNDERLIERS,
                });
            }
            if !seen.insert(f.underlier_symbol.clone()) {
                return Err(ConfigError::DuplicateUnderlier(f.underlier_symbol.clone()));
            }
        }
        if self.defaults.open_hours_cadence_secs == 0 {
            return Err(ConfigError::InvalidCadence(
                "open_hours_cadence_secs must be > 0".into(),
            ));
        }
        if let Some(c) = self.defaults.closed_hours_cadence_secs {
            if c == 0 {
                return Err(ConfigError::InvalidCadence(
                    "closed_hours_cadence_secs must be > 0 (or null to skip)".into(),
                ));
            }
        }
        if let Some(c) = self.defaults.weekend_cadence_secs {
            if c == 0 {
                return Err(ConfigError::InvalidCadence(
                    "weekend_cadence_secs must be > 0 (or null to skip)".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_methodology() {
        let d = FeedDefaults::default();
        assert_eq!(d.open_hours_cadence_secs, 60);
        assert_eq!(d.closed_hours_cadence_secs, Some(900));
        assert_eq!(d.weekend_cadence_secs, None);
        assert_eq!(d.skip_if_similar_bps, 5);
        assert_eq!(d.staleness_skip_threshold_secs, 300);
    }

    #[test]
    fn pilot_is_spy_only() {
        let c = PosterConfig::pilot_spy_placeholder();
        assert_eq!(c.feeds.len(), 1);
        assert_eq!(c.feeds[0].underlier_symbol, "SPY");
    }

    #[test]
    fn validate_accepts_permitted_tickers() {
        let c = PosterConfig {
            defaults: FeedDefaults::default(),
            feeds: vec![
                FeedConfig {
                    feed_id_hex: "a".repeat(64),
                    underlier_symbol: "SPY".into(),
                },
                FeedConfig {
                    feed_id_hex: "b".repeat(64),
                    underlier_symbol: "QQQ".into(),
                },
            ],
        };
        c.validate().expect("should accept");
    }

    #[test]
    fn validate_rejects_off_allowlist_ticker() {
        let c = PosterConfig {
            defaults: FeedDefaults::default(),
            feeds: vec![FeedConfig {
                feed_id_hex: "a".repeat(64),
                underlier_symbol: "AMZN".into(), // not in v0.1 list
            }],
        };
        let err = c.validate().unwrap_err();
        assert!(matches!(err, ConfigError::TickerNotPermitted { .. }));
    }

    #[test]
    fn validate_rejects_duplicate_underlier() {
        let c = PosterConfig {
            defaults: FeedDefaults::default(),
            feeds: vec![
                FeedConfig {
                    feed_id_hex: "a".repeat(64),
                    underlier_symbol: "SPY".into(),
                },
                FeedConfig {
                    feed_id_hex: "b".repeat(64),
                    underlier_symbol: "SPY".into(),
                },
            ],
        };
        let err = c.validate().unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateUnderlier(_)));
    }

    #[test]
    fn validate_rejects_zero_cadence() {
        let mut c = PosterConfig::pilot_spy_placeholder();
        c.feeds[0].feed_id_hex = "a".repeat(64);
        c.defaults.open_hours_cadence_secs = 0;
        assert!(matches!(c.validate(), Err(ConfigError::InvalidCadence(_))));
    }

    #[test]
    fn default_path_is_under_app_support() {
        let p = PosterConfig::default_path();
        let s = p.to_string_lossy();
        assert!(s.ends_with("scryer/config/pyth_poster_feeds.toml"), "got {s}");
    }

    #[test]
    fn permitted_underliers_match_methodology_locked_list() {
        // Mirror of the methodology entry's "Closed list at v0.1" so
        // the lock and the code stay in sync.
        let expected = [
            "SPY", "QQQ", "AAPL", "GOOGL", "NVDA", "TSLA", "HOOD", "MSTR", "GLD", "TLT",
        ];
        assert_eq!(V0_1_PERMITTED_UNDERLIERS, expected);
    }
}
