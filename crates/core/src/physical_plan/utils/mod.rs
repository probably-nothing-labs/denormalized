use std::result;

use datafusion_common::DataFusionError;

pub mod accumulators;
pub mod time;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;
