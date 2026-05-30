use std::num::TryFromIntError;

use jj_lib::backend::BackendError;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::OpStoreError;
use jj_lib::repo_path::InvalidNewRepoPathError;

#[derive(Debug, thiserror::Error)]
pub enum SqlBackendError {
    #[error("IO error")]
    IoError(#[from] std::io::Error),
    #[error("SQL error")]
    SqlError(#[from] rusqlite::Error),
    #[error("JSON error")]
    JsonError(#[from] serde_json::Error),
    #[error("Encoding error")]
    EncodingError(#[from] postcard::Error),
    #[error(
        "Invalid hash length for object of type {object_type} (expected {expected} bytes, got \
         {actual} bytes): {hash}"
    )]
    InvalidHashLength {
        expected: usize,
        actual: usize,
        object_type: String,
        hash: String,
    },
    #[error(transparent)]
    InvalidNewRepoPathError(#[from] InvalidNewRepoPathError),
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Error when reading object {hash} of type {object_type}")]
    ReadObject {
        object_type: String,
        hash: String,
        source: Box<SqlBackendError>,
    },
    #[error("Could not write object of type {object_type}")]
    WriteObject {
        object_type: &'static str,
        source: Box<SqlBackendError>,
    },
}

impl SqlBackendError {
    /// Error to be used when an the number of elements in an array exceeds the
    /// range of an i64.
    pub(crate) fn len_too_large(err: TryFromIntError) -> Self {
        Self::InternalError(format!("length too large: {err}"))
    }

    pub(crate) fn with_read_context(self, id: &impl ObjectId) -> Self {
        match self {
            hash_err @ Self::InvalidHashLength { .. } => hash_err,
            other => Self::ReadObject {
                object_type: id.object_type(),
                hash: id.hex(),
                source: Box::new(other),
            },
        }
    }

    pub(crate) fn with_write_context(self, object_type: &'static str) -> Self {
        match self {
            hash_err @ Self::InvalidHashLength { .. } => hash_err,
            other => Self::WriteObject {
                object_type,
                source: Box::new(other),
            },
        }
    }
}

impl From<SqlBackendError> for BackendInitError {
    fn from(value: SqlBackendError) -> Self {
        Self(Box::new(value))
    }
}

impl From<SqlBackendError> for BackendLoadError {
    fn from(value: SqlBackendError) -> Self {
        Self(Box::new(value))
    }
}

impl From<SqlBackendError> for BackendError {
    fn from(value: SqlBackendError) -> Self {
        match value {
            SqlBackendError::InvalidHashLength {
                expected,
                actual,
                object_type,
                hash,
            } => Self::InvalidHashLength {
                expected,
                actual,
                object_type,
                hash,
            },
            SqlBackendError::ReadObject {
                object_type,
                hash,
                source,
            } => match *source {
                SqlBackendError::SqlError(rusqlite::Error::QueryReturnedNoRows) => {
                    Self::ObjectNotFound {
                        object_type,
                        hash,
                        source,
                    }
                }
                _ => Self::ReadObject {
                    object_type,
                    hash,
                    source,
                },
            },
            SqlBackendError::WriteObject {
                object_type,
                source,
            } => Self::WriteObject {
                object_type,
                source,
            },
            other => Self::Other(Box::new(other)),
        }
    }
}

impl From<SqlBackendError> for OpStoreError {
    fn from(value: SqlBackendError) -> Self {
        match value {
            SqlBackendError::ReadObject {
                object_type,
                hash,
                source,
            } => match *source {
                SqlBackendError::SqlError(rusqlite::Error::QueryReturnedNoRows) => {
                    Self::ObjectNotFound {
                        object_type,
                        hash,
                        source,
                    }
                }
                _ => Self::ReadObject {
                    object_type,
                    hash,
                    source,
                },
            },
            SqlBackendError::WriteObject {
                object_type,
                source,
            } => Self::WriteObject {
                object_type,
                source,
            },
            other => Self::Other(Box::new(other)),
        }
    }
}
