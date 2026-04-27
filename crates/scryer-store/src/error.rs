use std::fmt;
use std::path::PathBuf;

#[derive(Debug)]
pub enum StoreError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Parquet(parquet::errors::ParquetError),
    Arrow(arrow_schema::ArrowError),
    Schema(scryer_schema::FromArrowError),
    /// Caller passed a `partition_key` that doesn't match the
    /// schema's `PARTITION_KEY_PREFIX`. E.g., calling
    /// `Dataset::write::<Swap>(venue, None, ...)` when swap.v1 is
    /// keyed by `pool=`, or `Dataset::write::<Pyth>(venue, Some(_), ...)`
    /// when pyth.v1 is no-key.
    PartitionKeyMismatch {
        schema: &'static str,
        expected_prefix: Option<&'static str>,
        provided_key: bool,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "io error at {}: {source}", path.display()),
            Self::Parquet(e) => write!(f, "parquet error: {e}"),
            Self::Arrow(e) => write!(f, "arrow error: {e}"),
            Self::Schema(e) => write!(f, "schema error: {e}"),
            Self::PartitionKeyMismatch { schema, expected_prefix, provided_key } => {
                match (expected_prefix, provided_key) {
                    (Some(p), false) => write!(
                        f,
                        "schema `{schema}` is keyed by `{p}=` but no partition_key was provided"
                    ),
                    (None, true) => write!(
                        f,
                        "schema `{schema}` is event-stream (no key) but a partition_key was provided"
                    ),
                    _ => write!(f, "internal partition-key state error"),
                }
            }
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parquet(e) => Some(e),
            Self::Arrow(e) => Some(e),
            Self::Schema(e) => Some(e),
            Self::PartitionKeyMismatch { .. } => None,
        }
    }
}

impl From<parquet::errors::ParquetError> for StoreError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Parquet(e)
    }
}

impl From<arrow_schema::ArrowError> for StoreError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Arrow(e)
    }
}

impl From<scryer_schema::FromArrowError> for StoreError {
    fn from(e: scryer_schema::FromArrowError) -> Self {
        Self::Schema(e)
    }
}
