pub mod accumulators;
pub mod config_extensions;
pub mod context;
pub mod datasource;
pub mod datastream;
pub mod formats;
pub mod logical_plan;
pub mod physical_optimizer;
pub mod physical_plan;
pub mod planner;
pub mod query_planner;
pub mod state_backend;
pub mod utils;

// re-export datafusion
pub use datafusion;

// re-export common module
pub mod common {
    pub use denormalized_common::*;
}

pub mod prelude {
    pub use crate::context::Context;
    pub use crate::datastream::DataStream;
    pub use crate::physical_plan::utils::time::TimestampUnit;

    pub use denormalized_common::{DenormalizedError, Result};

    pub use datafusion::dataframe::DataFrame;
}
