//! Source-manifest parser and validator.
//!
//! Manifests live under `ops/sources/<id>.toml` and declare every
//! source-fetcher cluster the runner will eventually drive. The
//! schema is locked in `methodology_log.md` under
//! `Source manifest format` (2026-05-02); the worked example is
//! `ops/sources/kraken-trades.toml`.
//!
//! Phase scope (M1.2): parse, validate, and surface every shape and
//! cross-field rule the methodology lock spells out. Sensor
//! evaluation, budget enforcement, freshness alerting, and workflow
//! execution are downstream milestones (M3.x); this crate is the
//! gate before any of them.
//!
//! Anti-rules enforced here:
//!
//! - `id` must be kebab-case and equal to the file stem.
//! - `[fetch].command` must be `"scry"` today.
//! - `[fetch].args` may not contain `--dataset` (the runner injects it).
//! - Every `schema_ids` entry must resolve to a v2 `SchemaId` or a
//!   `KNOWN_V1_SCHEMAS` entry.
//! - Unknown TOML keys are rejected (`#[serde(deny_unknown_fields)]`).

use std::path::{Path, PathBuf};

use scryer_schema::{is_known_v1_schema, SchemaId};
use serde::Deserialize;

pub mod error;
mod sensor;

pub use error::ManifestError;
pub use sensor::Sensor;

// `Criticality` and `Tier` are defined below; this re-export keeps
// PR.5 / PR.8 / runner consumers from having to know the inner
// module path.

/// Parsed and validated source manifest.
#[derive(Clone, Debug)]
pub struct Manifest {
    pub id: String,
    pub description: String,
    pub schema_ids: Vec<SchemaRef>,
    pub fetch: Fetch,
    pub freshness: Freshness,
    pub budget: Budget,
    /// Optional `[criticality]` block (PR.1). When present, the
    /// `tier` is a closed enum (`tier-0` through `tier-3`); `owner`
    /// and `consumer_impact` are freeform strings. Absence means
    /// "tier not declared yet"; downstream tier-aware behavior
    /// (PR.5/PR.8 alert routing) treats undeclared manifests as
    /// the lowest-priority bucket until they're tagged.
    pub criticality: Option<Criticality>,
    pub workflow: Option<Workflow>,
    pub depends_on: Vec<DependsOn>,
    /// Source path the manifest was loaded from, if any. `None` when
    /// the manifest came from a string.
    pub source_path: Option<PathBuf>,
}

/// One element of `schema_ids`. v2 entries decode into `SchemaId`;
/// v1 entries are preserved as the raw `<name>.v1` string because the
/// pre-taxonomy form is not representable as `SchemaId`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaRef {
    V1(String),
    V2(SchemaId),
}

impl SchemaRef {
    pub fn as_str(&self) -> String {
        match self {
            SchemaRef::V1(s) => s.clone(),
            SchemaRef::V2(id) => id.to_canonical_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Fetch {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Freshness {
    pub sla_secs: u64,
}

/// Per-axis budget caps. `None` means the axis is uncapped; the
/// runner is expected to log uncapped axes per the methodology lock.
#[derive(Clone, Debug, Default)]
pub struct Budget {
    pub max_requests_per_run: Option<u64>,
    pub max_provider_credits_per_run: Option<u64>,
    pub max_usd_per_day: Option<f64>,
}

impl Budget {
    /// Returns the uncapped axis names, in declaration order.
    pub fn uncapped_axes(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.max_requests_per_run.is_none() {
            out.push("max_requests_per_run");
        }
        if self.max_provider_credits_per_run.is_none() {
            out.push("max_provider_credits_per_run");
        }
        if self.max_usd_per_day.is_none() {
            out.push("max_usd_per_day");
        }
        out
    }
}

#[derive(Clone, Debug)]
pub struct Workflow {
    pub sensor_raw: String,
    pub sensor: Sensor,
    pub steps: Vec<Step>,
}

#[derive(Clone, Debug)]
pub struct Step {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DependsOn {
    pub id: String,
    pub fresh_within_secs: u64,
}

/// Source criticality block (PR.1). Optional today; future
/// tier-aware behavior (alert routing, retry budget) reads `tier`.
#[derive(Clone, Debug)]
pub struct Criticality {
    pub tier: Tier,
    /// Operator handle, freeform — e.g. `"@adam"`, `"oncall-data"`.
    pub owner: Option<String>,
    /// Freeform sentence describing what breaks downstream when
    /// this manifest is stale or failing. Surfaces in alert
    /// payloads so the responder doesn't have to look up impact.
    pub consumer_impact: Option<String>,
}

/// Closed source-criticality tier vocabulary (PR.1).
///
/// - `Tier0`: foundational data others depend on (oracle tapes,
///   block-level state). Stale = downstream blocked. Page-worthy.
/// - `Tier1`: primary research / production data. Stale = research
///   gap or trade-decision gap. Ticket-worthy with response SLA.
/// - `Tier2`: derived / analytics data (rollups, summaries,
///   audits). Stale = monitoring gap, not data gap.
/// - `Tier3`: experimental, one-off, or staged-rollout. Failures
///   don't page; surface in dashboards only.
///
/// Closed enum: adding a tier requires a methodology entry first
/// (the closure forces a deliberate decision when a new bucket
/// appears, same model as `Domain` in `scryer-schema`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tier {
    Tier0,
    Tier1,
    Tier2,
    Tier3,
}

impl Tier {
    pub const ALL: &'static [Tier] = &[Tier::Tier0, Tier::Tier1, Tier::Tier2, Tier::Tier3];

    pub const fn as_str(self) -> &'static str {
        match self {
            Tier::Tier0 => "tier-0",
            Tier::Tier1 => "tier-1",
            Tier::Tier2 => "tier-2",
            Tier::Tier3 => "tier-3",
        }
    }

    pub fn parse(s: &str) -> Result<Self, ManifestError> {
        for t in Self::ALL {
            if t.as_str() == s {
                return Ok(*t);
            }
        }
        Err(ManifestError::BadTier {
            value: s.to_owned(),
        })
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Manifest {
    /// Parse and validate a manifest from its TOML text. When
    /// `source_path` is provided, the parser also enforces the
    /// `<id>.toml` filename invariant.
    pub fn from_str(toml_text: &str, source_path: Option<&Path>) -> Result<Self, ManifestError> {
        let raw: RawManifest = toml::from_str(toml_text)?;
        validate(raw, source_path.map(Path::to_path_buf))
    }

    /// Read, parse, and validate a manifest from disk.
    pub fn from_path(path: &Path) -> Result<Self, ManifestError> {
        let text = std::fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Manifest::from_str(&text, Some(path))
    }
}

// ---------- raw TOML mirror types ----------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawManifest {
    id: String,
    description: String,
    schema_ids: Vec<String>,
    fetch: RawFetch,
    freshness: RawFreshness,
    #[serde(default)]
    budget: Option<RawBudget>,
    #[serde(default)]
    criticality: Option<RawCriticality>,
    #[serde(default)]
    workflow: Option<RawWorkflow>,
    #[serde(default, rename = "depends_on")]
    depends_on: Vec<RawDependsOn>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCriticality {
    tier: String,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    consumer_impact: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFetch {
    command: String,
    args: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFreshness {
    sla_secs: u64,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawBudget {
    #[serde(default)]
    max_requests_per_run: Option<u64>,
    #[serde(default)]
    max_provider_credits_per_run: Option<u64>,
    #[serde(default)]
    max_usd_per_day: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWorkflow {
    sensor: String,
    #[serde(default)]
    steps: Option<Vec<RawStep>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStep {
    command: String,
    args: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDependsOn {
    id: String,
    fresh_within_secs: u64,
}

// ---------- validation ----------

fn validate(raw: RawManifest, source_path: Option<PathBuf>) -> Result<Manifest, ManifestError> {
    validate_id(&raw.id)?;
    if let Some(path) = source_path.as_deref() {
        validate_id_matches_path(&raw.id, path)?;
    }

    if raw.schema_ids.is_empty() {
        return Err(ManifestError::EmptySchemaIds);
    }
    let schema_ids = raw
        .schema_ids
        .iter()
        .map(|s| resolve_schema_ref(s))
        .collect::<Result<Vec<_>, _>>()?;

    let fetch = validate_fetch(raw.fetch)?;
    let freshness = validate_freshness(raw.freshness)?;
    let budget = validate_budget(raw.budget.unwrap_or_default())?;
    let criticality = match raw.criticality {
        Some(c) => Some(validate_criticality(c)?),
        None => None,
    };

    let workflow = match raw.workflow {
        Some(w) => Some(validate_workflow(w)?),
        None => None,
    };

    let depends_on = raw
        .depends_on
        .into_iter()
        .map(validate_depends_on)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Manifest {
        id: raw.id,
        description: raw.description,
        schema_ids,
        fetch,
        freshness,
        budget,
        criticality,
        workflow,
        depends_on,
        source_path,
    })
}

fn validate_id(id: &str) -> Result<(), ManifestError> {
    if id.is_empty() {
        return Err(ManifestError::BadId {
            id: id.to_owned(),
            reason: "id may not be empty",
        });
    }
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return Err(ManifestError::BadId {
            id: id.to_owned(),
            reason: "id may not be empty",
        });
    };
    if !first.is_ascii_lowercase() {
        return Err(ManifestError::BadId {
            id: id.to_owned(),
            reason: "id must start with a lowercase letter",
        });
    }
    let mut prev_dash = false;
    for c in std::iter::once(first).chain(chars) {
        let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        if !ok {
            return Err(ManifestError::BadId {
                id: id.to_owned(),
                reason: "id must match [a-z][a-z0-9-]* (kebab-case)",
            });
        }
        if c == '-' && prev_dash {
            return Err(ManifestError::BadId {
                id: id.to_owned(),
                reason: "id may not contain consecutive `--`",
            });
        }
        prev_dash = c == '-';
    }
    if id.ends_with('-') {
        return Err(ManifestError::BadId {
            id: id.to_owned(),
            reason: "id may not end with `-`",
        });
    }
    Ok(())
}

fn validate_id_matches_path(id: &str, path: &Path) -> Result<(), ManifestError> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_owned();
    if stem != id {
        return Err(ManifestError::IdFileStemMismatch {
            id: id.to_owned(),
            file_stem: stem,
        });
    }
    Ok(())
}

fn resolve_schema_ref(value: &str) -> Result<SchemaRef, ManifestError> {
    if is_known_v1_schema(value) {
        return Ok(SchemaRef::V1(value.to_owned()));
    }
    if let Ok(id) = SchemaId::parse(value) {
        return Ok(SchemaRef::V2(id));
    }
    Err(ManifestError::UnknownSchemaId {
        value: value.to_owned(),
    })
}

fn validate_fetch(raw: RawFetch) -> Result<Fetch, ManifestError> {
    if raw.command != "scry" {
        return Err(ManifestError::UnsupportedFetchCommand { got: raw.command });
    }
    if raw.args.iter().any(|a| a == "--dataset") {
        return Err(ManifestError::FetchArgsContainDataset);
    }
    Ok(Fetch {
        command: raw.command,
        args: raw.args,
    })
}

fn validate_freshness(raw: RawFreshness) -> Result<Freshness, ManifestError> {
    if raw.sla_secs == 0 {
        return Err(ManifestError::BadFreshnessSla);
    }
    Ok(Freshness {
        sla_secs: raw.sla_secs,
    })
}

fn validate_budget(raw: RawBudget) -> Result<Budget, ManifestError> {
    if matches!(raw.max_requests_per_run, Some(0)) {
        return Err(ManifestError::BadBudgetField {
            field: "max_requests_per_run",
        });
    }
    if matches!(raw.max_provider_credits_per_run, Some(0)) {
        return Err(ManifestError::BadBudgetField {
            field: "max_provider_credits_per_run",
        });
    }
    if let Some(v) = raw.max_usd_per_day {
        if !(v > 0.0 && v.is_finite()) {
            return Err(ManifestError::BadBudgetField {
                field: "max_usd_per_day",
            });
        }
    }
    Ok(Budget {
        max_requests_per_run: raw.max_requests_per_run,
        max_provider_credits_per_run: raw.max_provider_credits_per_run,
        max_usd_per_day: raw.max_usd_per_day,
    })
}

fn validate_workflow(raw: RawWorkflow) -> Result<Workflow, ManifestError> {
    let sensor = Sensor::parse(&raw.sensor)?;
    let steps = match raw.steps {
        Some(list) => {
            let mut out = Vec::with_capacity(list.len());
            for (idx, step) in list.into_iter().enumerate() {
                if step.command != "scry" {
                    return Err(ManifestError::UnsupportedStepCommand {
                        index: idx,
                        got: step.command,
                    });
                }
                if step.args.iter().any(|a| a == "--dataset") {
                    return Err(ManifestError::StepArgsContainDataset { index: idx });
                }
                out.push(Step {
                    command: step.command,
                    args: step.args,
                });
            }
            out
        }
        None => Vec::new(),
    };
    Ok(Workflow {
        sensor_raw: raw.sensor,
        sensor,
        steps,
    })
}

fn validate_criticality(raw: RawCriticality) -> Result<Criticality, ManifestError> {
    let tier = Tier::parse(raw.tier.trim())?;
    // Owner / consumer_impact are freeform but must be non-empty
    // when declared — the empty string carries no information and
    // would just clutter alert payloads.
    let owner = match raw.owner {
        Some(s) if s.trim().is_empty() => return Err(ManifestError::EmptyCriticalityField {
            field: "owner",
        }),
        Some(s) => Some(s.trim().to_owned()),
        None => None,
    };
    let consumer_impact = match raw.consumer_impact {
        Some(s) if s.trim().is_empty() => return Err(ManifestError::EmptyCriticalityField {
            field: "consumer_impact",
        }),
        Some(s) => Some(s.trim().to_owned()),
        None => None,
    };
    Ok(Criticality {
        tier,
        owner,
        consumer_impact,
    })
}

fn validate_depends_on(raw: RawDependsOn) -> Result<DependsOn, ManifestError> {
    if raw.id.is_empty() {
        return Err(ManifestError::BadDependsOn);
    }
    if raw.fresh_within_secs == 0 {
        return Err(ManifestError::BadDependsOnFreshWithin);
    }
    Ok(DependsOn {
        id: raw.id,
        fresh_within_secs: raw.fresh_within_secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path(name: &str) -> PathBuf {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.pop();
        p.pop();
        p.push("ops/sources");
        p.push(name);
        p
    }

    #[test]
    fn parses_kraken_trades_worked_example() {
        let path = fixture_path("kraken-trades.toml");
        let m = Manifest::from_path(&path).expect("kraken-trades.toml must parse");
        assert_eq!(m.id, "kraken-trades");
        assert_eq!(m.schema_ids.len(), 1);
        assert_eq!(m.schema_ids[0], SchemaRef::V1("trade.v1".to_owned()));
        assert_eq!(m.fetch.command, "scry");
        assert_eq!(m.freshness.sla_secs, 7200);
        assert_eq!(m.budget.max_requests_per_run, Some(500));
        assert_eq!(
            m.budget.uncapped_axes(),
            vec!["max_provider_credits_per_run", "max_usd_per_day"]
        );
        let wf = m.workflow.as_ref().expect("workflow present");
        assert_eq!(wf.sensor, Sensor::Interval { secs: 3600 });
        assert!(wf.steps.is_empty());
        assert_eq!(m.source_path.as_deref(), Some(path.as_path()));
    }

    #[test]
    fn requires_id_to_match_filename() {
        let toml = r#"
id = "not-kraken-trades"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
"#;
        let path = PathBuf::from("/tmp/kraken-trades.toml");
        let err = Manifest::from_str(toml, Some(&path)).unwrap_err();
        assert!(matches!(err, ManifestError::IdFileStemMismatch { .. }));
    }

    #[test]
    fn accepts_v2_schema_ids() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["solana.kamino.liquidation.v2"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
"#;
        let m = Manifest::from_str(toml, None).unwrap();
        match &m.schema_ids[0] {
            SchemaRef::V2(id) => assert_eq!(id.to_canonical_string(), "solana.kamino.liquidation.v2"),
            _ => panic!("expected V2 schema ref"),
        }
    }

    #[test]
    fn rejects_unknown_schema_id() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["not_a_schema.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::UnknownSchemaId { .. }));
    }

    #[test]
    fn rejects_non_scry_fetch_command() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "curl"
args = []
[freshness]
sla_secs = 60
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::UnsupportedFetchCommand { .. }));
    }

    #[test]
    fn rejects_dataset_in_fetch_args() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = ["--dataset", "/tmp/x"]
[freshness]
sla_secs = 60
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::FetchArgsContainDataset));
    }

    #[test]
    fn rejects_unknown_top_level_key() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
extra = 1
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::Toml(_)));
    }

    #[test]
    fn rejects_zero_freshness() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 0
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::BadFreshnessSla));
    }

    #[test]
    fn rejects_zero_budget_axis() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
[budget]
max_requests_per_run = 0
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::BadBudgetField { .. }));
    }

    #[test]
    fn rejects_kebab_id_with_underscores() {
        let toml = r#"
id = "kraken_trades"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::BadId { .. }));
    }

    #[test]
    fn rejects_empty_schema_ids() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = []
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
"#;
        let err = Manifest::from_str(toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::EmptySchemaIds));
    }

    #[test]
    fn parses_depends_on() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
[[depends_on]]
id = "kraken-trades"
fresh_within_secs = 7200
"#;
        let m = Manifest::from_str(toml, None).unwrap();
        assert_eq!(m.depends_on.len(), 1);
        assert_eq!(m.depends_on[0].id, "kraken-trades");
        assert_eq!(m.depends_on[0].fresh_within_secs, 7200);
    }

    // ============================================================
    // PR.1 — criticality block
    // ============================================================

    fn base_manifest_with_criticality(block: &str) -> String {
        format!(
            r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = []
[freshness]
sla_secs = 60
{block}
"#
        )
    }

    #[test]
    fn criticality_is_optional() {
        // Existing manifests that don't declare [criticality] still
        // parse cleanly.
        let toml = base_manifest_with_criticality("");
        let m = Manifest::from_str(&toml, None).unwrap();
        assert!(m.criticality.is_none());
    }

    #[test]
    fn parses_criticality_with_full_block() {
        let toml = base_manifest_with_criticality(
            r#"
[criticality]
tier = "tier-0"
owner = "@adam"
consumer_impact = "Foundational oracle tape used by Paper-3 and LVR research."
"#,
        );
        let m = Manifest::from_str(&toml, None).unwrap();
        let c = m.criticality.expect("criticality present");
        assert_eq!(c.tier, Tier::Tier0);
        assert_eq!(c.owner.as_deref(), Some("@adam"));
        assert!(c
            .consumer_impact
            .as_deref()
            .unwrap()
            .starts_with("Foundational"));
    }

    #[test]
    fn parses_criticality_with_only_tier() {
        let toml = base_manifest_with_criticality(
            r#"
[criticality]
tier = "tier-2"
"#,
        );
        let m = Manifest::from_str(&toml, None).unwrap();
        let c = m.criticality.unwrap();
        assert_eq!(c.tier, Tier::Tier2);
        assert!(c.owner.is_none());
        assert!(c.consumer_impact.is_none());
    }

    #[test]
    fn rejects_unknown_tier_value() {
        let toml = base_manifest_with_criticality(
            r#"
[criticality]
tier = "critical"
"#,
        );
        let err = Manifest::from_str(&toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::BadTier { .. }));
    }

    #[test]
    fn rejects_empty_owner_field() {
        let toml = base_manifest_with_criticality(
            r#"
[criticality]
tier = "tier-1"
owner = ""
"#,
        );
        let err = Manifest::from_str(&toml, None).unwrap_err();
        assert!(matches!(
            err,
            ManifestError::EmptyCriticalityField { field: "owner" }
        ));
    }

    #[test]
    fn rejects_empty_consumer_impact_field() {
        let toml = base_manifest_with_criticality(
            r#"
[criticality]
tier = "tier-1"
consumer_impact = "   "
"#,
        );
        let err = Manifest::from_str(&toml, None).unwrap_err();
        assert!(matches!(
            err,
            ManifestError::EmptyCriticalityField {
                field: "consumer_impact"
            }
        ));
    }

    #[test]
    fn rejects_unknown_keys_in_criticality_block() {
        let toml = base_manifest_with_criticality(
            r#"
[criticality]
tier = "tier-1"
oncall = "yes"
"#,
        );
        let err = Manifest::from_str(&toml, None).unwrap_err();
        assert!(matches!(err, ManifestError::Toml(_)));
    }

    #[test]
    fn tier_round_trips_through_str() {
        for t in Tier::ALL {
            assert_eq!(Tier::parse(t.as_str()).unwrap(), *t);
        }
    }

    #[test]
    fn parses_workflow_steps() {
        let toml = r#"
id = "demo"
description = "x"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = ["kraken", "trades"]
[freshness]
sla_secs = 60
[workflow]
sensor = "interval(60s)"
[[workflow.steps]]
command = "scry"
args = ["kraken", "trades", "--pair", "SOLUSD"]
"#;
        let m = Manifest::from_str(toml, None).unwrap();
        let wf = m.workflow.unwrap();
        assert_eq!(wf.steps.len(), 1);
        assert_eq!(wf.steps[0].args[0], "kraken");
    }
}
