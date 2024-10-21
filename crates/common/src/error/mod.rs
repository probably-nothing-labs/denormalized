use std::result;
use thiserror::Error;

use apache_avro::Error as AvroError;
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use serde_json::Error as JsonError;
#[cfg(feature = "python")]
mod py_err;

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
    #[error("Avro Error")]
    AvroError(#[from] AvroError),
    #[error("Json Error")]
    Json(#[from] JsonError),
    //#[error("SlateDB Error")]
    //SlateDBError(#[from] SlateDBError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
