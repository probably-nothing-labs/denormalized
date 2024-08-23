use std::result;
use thiserror::Error;

use datafusion::error::DataFusionError;
use arrow::error::ArrowError;

/// Result type for operations that could result in a [DenormalizedError]
pub type Result<T, E = DenormalizedError> = result::Result<T, E>;

/// Denormalized Error
#[derive(Error, Debug)]
pub enum DenormalizedError {
    // #[allow(clippy::disallowed_types)]
    #[error("DataFusion error")]
    DataFusion(#[from] DataFusionError),
    #[error("RocksDB error: {0}")]
    RocksDB(String),
    #[error("Kafka error")]
    KafkaConfig(String),
    #[error("Arrow Error")]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
