//! Typed schema identifiers for the v0.2 namespace.
//!
//! v2 schema IDs use the form `<domain>.<source>.<record_type>.v<n>` per
//! the locked "Schema namespace taxonomy" methodology entry. v1 IDs
//! (`swap.v1`, `trade.v1`, etc.) remain in their original two-part form
//! and are not represented by this type — see the migration index in
//! `docs/platform_plan.md`.
//!
//! `Domain` is a closed enum. Adding a domain requires a methodology
//! update first (the closure is the point: it forces a deliberate
//! decision when a new category appears).
//!
//! `KNOWN_V2_SCHEMAS` is the registry of every v2 ID that has been
//! locked or shipped. Uniqueness across the registry is checked at
//! `cargo test` time; merging a new v2 schema without registering it
//! here is an error by convention. The list starts empty because no v2
//! schemas have shipped yet; entries land as Wave 1+ migrations land.

use std::borrow::Cow;
use std::fmt;

/// Closed enum of v2 domain values. Order matches the methodology entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Domain {
    Solana,
    Evm,
    Cex,
    DexAgg,
    Oracle,
    Equity,
    Macro,
    News,
    TradfiDeriv,
    Volatility,
    Internal,
}

impl Domain {
    pub const ALL: &'static [Domain] = &[
        Domain::Solana,
        Domain::Evm,
        Domain::Cex,
        Domain::DexAgg,
        Domain::Oracle,
        Domain::Equity,
        Domain::Macro,
        Domain::News,
        Domain::TradfiDeriv,
        Domain::Volatility,
        Domain::Internal,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Domain::Solana => "solana",
            Domain::Evm => "evm",
            Domain::Cex => "cex",
            Domain::DexAgg => "dex_agg",
            Domain::Oracle => "oracle",
            Domain::Equity => "equity",
            Domain::Macro => "macro",
            Domain::News => "news",
            Domain::TradfiDeriv => "tradfi_deriv",
            Domain::Volatility => "volatility",
            Domain::Internal => "internal",
        }
    }

    pub fn parse(s: &str) -> Result<Self, SchemaIdError> {
        for d in Self::ALL {
            if d.as_str() == s {
                return Ok(*d);
            }
        }
        Err(SchemaIdError::UnknownDomain(s.to_owned()))
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Typed v2 schema identifier.
///
/// `source` is provider/protocol/venue or the reserved literal
/// `"aggregate"` for cross-source panels. `record_type` follows the
/// controlled vocabulary in `docs/schemas.md`. Both segments must match
/// `[a-z][a-z0-9_]*` (lowercase, no leading digit, no dots).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaId {
    pub domain: Domain,
    pub source: Cow<'static, str>,
    pub record_type: Cow<'static, str>,
    pub version: u32,
}

impl SchemaId {
    /// Const-friendly construction for static registry entries. The
    /// caller is responsible for passing valid segments; format
    /// validation runs in `KNOWN_V2_SCHEMAS` tests, not at this
    /// callsite, because `const fn` cannot validate strings.
    pub const fn new_static(
        domain: Domain,
        source: &'static str,
        record_type: &'static str,
        version: u32,
    ) -> Self {
        Self {
            domain,
            source: Cow::Borrowed(source),
            record_type: Cow::Borrowed(record_type),
            version,
        }
    }

    /// Parse and validate a string form like `solana.kamino.liquidation.v2`.
    pub fn parse(s: &str) -> Result<Self, SchemaIdError> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 4 {
            return Err(SchemaIdError::WrongShape(s.to_owned()));
        }
        let domain = Domain::parse(parts[0])?;
        validate_segment("source", parts[1])?;
        validate_segment("record_type", parts[2])?;
        let version_part = parts[3];
        let version_digits = version_part
            .strip_prefix('v')
            .ok_or_else(|| SchemaIdError::BadVersion(version_part.to_owned()))?;
        let version: u32 = version_digits
            .parse()
            .map_err(|_| SchemaIdError::BadVersion(version_part.to_owned()))?;
        if version == 0 {
            return Err(SchemaIdError::BadVersion(version_part.to_owned()));
        }
        Ok(Self {
            domain,
            source: Cow::Owned(parts[1].to_owned()),
            record_type: Cow::Owned(parts[2].to_owned()),
            version,
        })
    }

    pub fn to_canonical_string(&self) -> String {
        format!(
            "{}.{}.{}.v{}",
            self.domain.as_str(),
            self.source,
            self.record_type,
            self.version,
        )
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}.v{}",
            self.domain.as_str(),
            self.source,
            self.record_type,
            self.version,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaIdError {
    WrongShape(String),
    UnknownDomain(String),
    BadSegment {
        kind: &'static str,
        value: String,
    },
    BadVersion(String),
}

impl fmt::Display for SchemaIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongShape(s) => write!(
                f,
                "schema id `{s}` is not in <domain>.<source>.<record_type>.v<n> form"
            ),
            Self::UnknownDomain(s) => {
                write!(f, "schema id domain `{s}` is not in the closed domain enum")
            }
            Self::BadSegment { kind, value } => write!(
                f,
                "schema id {kind} segment `{value}` must match [a-z][a-z0-9_]*"
            ),
            Self::BadVersion(s) => write!(f, "schema id version `{s}` must be `v<n>` with n >= 1"),
        }
    }
}

impl std::error::Error for SchemaIdError {}

fn validate_segment(kind: &'static str, segment: &str) -> Result<(), SchemaIdError> {
    let bad = || SchemaIdError::BadSegment {
        kind,
        value: segment.to_owned(),
    };
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return Err(bad());
    };
    if !first.is_ascii_lowercase() {
        return Err(bad());
    }
    for c in chars {
        let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_';
        if !ok {
            return Err(bad());
        }
    }
    Ok(())
}

/// Registry of every v2 schema ID locked or shipped to date.
///
/// The list is empty until Wave-1 migrations begin. Adding an entry is
/// part of merging the v2 schema; the `known_v2_schemas_*` tests
/// enforce that every entry parses, validates, and is unique.
pub const KNOWN_V2_SCHEMAS: &[SchemaId] = &[];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn round_trip_parse_format() {
        let s = "solana.kamino.liquidation.v2";
        let id = SchemaId::parse(s).unwrap();
        assert_eq!(id.domain, Domain::Solana);
        assert_eq!(id.source, "kamino");
        assert_eq!(id.record_type, "liquidation");
        assert_eq!(id.version, 2);
        assert_eq!(id.to_string(), s);
    }

    #[test]
    fn aggregate_source_is_allowed() {
        let id = SchemaId::parse("cex.aggregate.perp_funding.v2").unwrap();
        assert_eq!(id.source, "aggregate");
    }

    #[test]
    fn rejects_unknown_domain() {
        assert!(matches!(
            SchemaId::parse("perps.binance.trade.v2"),
            Err(SchemaIdError::UnknownDomain(_))
        ));
    }

    #[test]
    fn rejects_v1_two_part_form() {
        assert!(matches!(
            SchemaId::parse("trade.v1"),
            Err(SchemaIdError::WrongShape(_))
        ));
    }

    #[test]
    fn rejects_uppercase_segment() {
        assert!(matches!(
            SchemaId::parse("solana.Kamino.liquidation.v2"),
            Err(SchemaIdError::BadSegment { .. })
        ));
    }

    #[test]
    fn rejects_zero_version() {
        assert!(matches!(
            SchemaId::parse("solana.kamino.liquidation.v0"),
            Err(SchemaIdError::BadVersion(_))
        ));
    }

    #[test]
    fn rejects_missing_v_prefix() {
        assert!(matches!(
            SchemaId::parse("solana.kamino.liquidation.2"),
            Err(SchemaIdError::BadVersion(_))
        ));
    }

    #[test]
    fn rejects_segment_with_dot_or_hyphen() {
        assert!(SchemaId::parse("solana.kamino.lend-pool.v2").is_err());
        assert!(SchemaId::parse("solana.kamino.lend.pool.v2").is_err());
    }

    /// Every entry in the v2 registry parses cleanly.
    ///
    /// The registry holds pre-validated `SchemaId` values via
    /// `new_static`, but the constructor cannot enforce segment shape
    /// at compile time. Round-tripping through `parse` is the gate.
    #[test]
    fn known_v2_schemas_all_parse() {
        for id in KNOWN_V2_SCHEMAS {
            let s = id.to_canonical_string();
            let reparsed = SchemaId::parse(&s)
                .unwrap_or_else(|e| panic!("registry id {s} fails parse: {e}"));
            assert_eq!(*id, reparsed, "registry id {s} fails round-trip");
        }
    }

    /// No duplicate canonical strings across the v2 registry.
    #[test]
    fn known_v2_schemas_unique() {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for id in KNOWN_V2_SCHEMAS {
            let s = id.to_canonical_string();
            assert!(seen.insert(s.clone()), "duplicate schema id in registry: {s}");
        }
    }
}
