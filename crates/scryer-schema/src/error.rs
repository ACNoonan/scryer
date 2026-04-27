use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromArrowError {
    MissingColumn(&'static str),
    WrongType {
        column: &'static str,
        expected: &'static str,
    },
    SchemaVersionMismatch {
        expected: &'static str,
        found: String,
    },
    UnknownEnumValue {
        column: &'static str,
        value: String,
    },
}

impl fmt::Display for FromArrowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingColumn(c) => write!(f, "missing column `{c}`"),
            Self::WrongType { column, expected } => {
                write!(f, "column `{column}` has wrong type (expected {expected})")
            }
            Self::SchemaVersionMismatch { expected, found } => write!(
                f,
                "schema version mismatch: expected `{expected}`, found `{found}`"
            ),
            Self::UnknownEnumValue { column, value } => {
                write!(f, "unknown value `{value}` in column `{column}`")
            }
        }
    }
}

impl std::error::Error for FromArrowError {}
