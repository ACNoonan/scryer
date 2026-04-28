//! Default FRED release registry — the canonical macro indicators
//! used as regime regressors in soothsayer's calibration pipeline.

#[derive(Clone, Copy, Debug)]
pub struct ReleaseEntry {
    /// FRED's numeric release ID.
    pub release_id: i32,
    /// Canonical short name written into the schema's `event_name`
    /// column. Stable across upstream renames.
    pub event_name: &'static str,
    /// Full FRED release name; used as the schema's `release_name`
    /// fallback when the upstream response omits the field.
    pub upstream_name: &'static str,
}

/// Six-release default. Mapping from FRED's release IDs to canonical
/// short names. Verified against `https://api.stlouisfed.org/fred/releases`.
pub const DEFAULT_RELEASES: &[ReleaseEntry] = &[
    ReleaseEntry {
        release_id: 10,
        event_name: "CPI",
        upstream_name: "Consumer Price Index",
    },
    ReleaseEntry {
        release_id: 50,
        event_name: "NFP",
        upstream_name: "Employment Situation",
    },
    ReleaseEntry {
        release_id: 53,
        event_name: "GDP",
        upstream_name: "Gross Domestic Product",
    },
    ReleaseEntry {
        release_id: 21,
        event_name: "PCE",
        upstream_name: "Personal Income and Outlays",
    },
    ReleaseEntry {
        release_id: 84,
        event_name: "PPI",
        upstream_name: "Producer Price Index",
    },
    ReleaseEntry {
        release_id: 32,
        event_name: "RetailSales",
        upstream_name: "Retail Trade",
    },
];

/// Look up the default registry by release_id; returns `None` for
/// unknown IDs (e.g. when the CLI's `--release-ids` flag includes an
/// ID outside the default set, the caller falls back to a synthesized
/// `event_name = "release_<id>"`).
pub fn lookup(release_id: i32) -> Option<&'static ReleaseEntry> {
    DEFAULT_RELEASES
        .iter()
        .find(|e| e.release_id == release_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_set_has_six_releases() {
        assert_eq!(DEFAULT_RELEASES.len(), 6);
    }

    #[test]
    fn default_set_has_unique_ids_and_names() {
        use std::collections::BTreeSet;
        let ids: BTreeSet<_> = DEFAULT_RELEASES.iter().map(|e| e.release_id).collect();
        let names: BTreeSet<_> = DEFAULT_RELEASES.iter().map(|e| e.event_name).collect();
        assert_eq!(ids.len(), DEFAULT_RELEASES.len(), "duplicate release_id");
        assert_eq!(names.len(), DEFAULT_RELEASES.len(), "duplicate event_name");
    }

    #[test]
    fn lookup_finds_known_id() {
        assert_eq!(lookup(10).unwrap().event_name, "CPI");
        assert_eq!(lookup(50).unwrap().event_name, "NFP");
    }

    #[test]
    fn lookup_misses_unknown_id() {
        assert!(lookup(99999).is_none());
    }
}
