use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("scan manifests in {path}: {reason}")]
    ManifestScan { path: PathBuf, reason: String },

    #[error("load manifest at {path}: {source}")]
    Manifest {
        path: PathBuf,
        #[source]
        source: scryer_manifest::ManifestError,
    },

    #[error("no manifest with id `{id}` is loaded")]
    UnknownManifestId { id: String },

    #[error("read state file {path}: {reason}")]
    StateRead { path: PathBuf, reason: String },

    #[error("write state file {path}: {reason}")]
    StateWrite { path: PathBuf, reason: String },

    #[error("write workflow_run row: {reason}")]
    SinkWrite { reason: String },

    #[error("spawn `{command}`: {reason}")]
    Spawn { command: String, reason: String },
}
