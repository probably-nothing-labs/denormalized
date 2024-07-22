use std::str::FromStr;

use arrow_schema::SchemaRef;
use datafusion_physical_plan::time::TimestampUnit;

use datafusion_common::{plan_err, DataFusionError};
use datafusion_expr::Expr;

/// The configuration for a [`StreamTable`]
#[derive(Debug)]
pub struct KafkaTopicConfig {
    pub topic: String,
    pub bootstrap_servers: String,

    pub original_schema: SchemaRef,
    pub schema: SchemaRef,
    pub encoding: StreamEncoding,
    pub order: Vec<Vec<Expr>>,
    pub partitions: i32,
    pub timestamp_column: String,
    pub timestamp_unit: TimestampUnit,
    pub consumer_group_id: String,
    pub offset_reset: String,
}

impl KafkaTopicConfig {
}

/// The data encoding for [`StreamTable`]
#[derive(Debug, Clone)]
pub enum StreamEncoding {
    Avro,
    Json,
}

impl FromStr for StreamEncoding {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "json" => Ok(Self::Json),
            _ => plan_err!("Unrecognised StreamEncoding {}", s),
        }
    }
}
