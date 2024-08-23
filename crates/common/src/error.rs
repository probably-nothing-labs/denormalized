use std::result;
use datafusion::error::DataFusionError;
use thiserror::Error;

/// Result type for operations that could result in a [DenormalizedError]
pub type Result<T, E = DataFusionError> = result::Result<T, E>;

/// Denormalized Error
#[derive(Error, Debug)]
pub enum DenormalizedError {
    #[error("DataFusion error")]
    DataFusion(#[from] DataFusionError),
}
