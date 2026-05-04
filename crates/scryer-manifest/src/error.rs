use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("read manifest at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parse TOML: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("manifest id `{id}` is invalid: {reason}")]
    BadId { id: String, reason: &'static str },

    #[error(
        "manifest id `{id}` does not match file stem `{file_stem}`; \
        the methodology lock requires <id>.toml"
    )]
    IdFileStemMismatch { id: String, file_stem: String },

    #[error("schema_ids may not be empty")]
    EmptySchemaIds,

    #[error(
        "schema_ids entry `{value}` is neither a v2 SchemaId nor a known v1 schema string"
    )]
    UnknownSchemaId { value: String },

    #[error("[fetch].command must be `\"scry\"`; got `{got}`")]
    UnsupportedFetchCommand { got: String },

    #[error("[fetch].args may not contain `--dataset`; the runner injects it")]
    FetchArgsContainDataset,

    #[error("[freshness].sla_secs must be >= 1")]
    BadFreshnessSla,

    #[error(
        "[budget].{field} must be >= 1; omit the key to declare the axis uncapped"
    )]
    BadBudgetField { field: &'static str },

    #[error(
        "[criticality].tier value `{value}` is not in the closed enum (tier-0..tier-3)"
    )]
    BadTier { value: String },

    #[error("[criticality].{field} declared but empty; omit the key instead")]
    EmptyCriticalityField { field: &'static str },

    #[error("[[depends_on]] entry must declare both `id` and `fresh_within_secs`")]
    BadDependsOn,

    #[error("[[depends_on]].fresh_within_secs must be >= 1")]
    BadDependsOnFreshWithin,

    #[error("[workflow].sensor `{raw}` malformed: {reason}")]
    SensorShape { raw: String, reason: &'static str },

    #[error("[workflow].sensor `{raw}` has unknown kind `{kind}`")]
    SensorUnknownKind { raw: String, kind: String },

    #[error(
        "[workflow].sensor `{raw}` references unknown schema id `{schema_id}`"
    )]
    SensorUnknownSchema { raw: String, schema_id: String },

    #[error("[workflow].steps[{index}].command must be `\"scry\"`; got `{got}`")]
    UnsupportedStepCommand { index: usize, got: String },

    #[error("[workflow].steps[{index}].args may not contain `--dataset`")]
    StepArgsContainDataset { index: usize },

    #[error(
        "[retry].retry_on entry `{value}` is not in the closed enum (transient, timeout, nonzero_exit)"
    )]
    BadRetryOn { value: String },

    #[error("[retry].{field}: {reason}")]
    BadRetryField {
        field: &'static str,
        reason: &'static str,
    },
}
