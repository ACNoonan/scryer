use serde::{Deserialize, Serialize};

/// Per-row metadata columns carried by every scryer schema.
///
/// Field names use leading-underscore JSON serialization so that
/// Rust-side struct fields and Python-side parquet column names agree
/// on what is logical-data versus metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    #[serde(rename = "_schema_version")]
    pub schema_version: String,
    #[serde(rename = "_fetched_at")]
    pub fetched_at: i64,
    #[serde(rename = "_source")]
    pub source: String,
}

impl Meta {
    pub fn new(
        schema_version: impl Into<String>,
        fetched_at: i64,
        source: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: schema_version.into(),
            fetched_at,
            source: source.into(),
        }
    }
}
