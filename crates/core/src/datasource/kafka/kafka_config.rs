use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion_physical_plan::time::TimestampUnit;

use crate::arrow_helpers::infer_arrow_schema_from_json_value;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::Expr;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Timestamp, TopicPartitionList};

pub type ConnectionOpts = HashMap<String, String>;

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

    pub consumer_opts: ConnectionOpts,
    pub producer_opts: ConnectionOpts,
}

impl KafkaTopicConfig {
    pub fn make_consumer(&self) -> StreamConsumer {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", self.bootstrap_servers.to_string())
            .set("enable.auto.commit", "false");

        for (key, value) in self.consumer_opts.clone().into_iter() {
            client_config.set(key, value);
        };

        let consumer: StreamConsumer = client_config.create().expect("Consumer creation failed");
        consumer
    }
    pub fn make_producer(&self) {}
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

pub struct KafkaTopicConfigBuilder {
    bootstrap_servers: String,
    topic: String,

    schema: Option<SchemaRef>,
    infer_schema: bool,

    encoding: Option<StreamEncoding>,
    consumer_opts: Option<ConnectionOpts>,
    producer_opts: Option<ConnectionOpts>,
}

impl KafkaTopicConfigBuilder {
    pub fn new(bootstrap_servers: String, topic: String) -> Self {
        Self {
            bootstrap_servers,
            topic,

            schema: None,
            infer_schema: false,

            encoding: None,
            consumer_opts: None,
            producer_opts: None,
        }
    }

    pub fn with_schema(&mut self, schema: SchemaRef) -> &Self {
        self.infer_schema = false;
        self.schema = Some(schema);
        self
    }

    pub fn infer_schema(&mut self) -> &Self {
        self.infer_schema = true;
        self
    }

    pub fn with_consumer_opts(&mut self, opts: ConnectionOpts) -> &Self {
        self.consumer_opts = Some(opts);
        self
    }

    pub fn with_producer_opts(&mut self, opts: ConnectionOpts) -> &Self {
        self.consumer_opts = Some(opts);
        self
    }

    pub fn infer_schema_from_json(&mut self, json: &str) -> Result<&Self> {
        self.infer_schema = false;

        let sample_value: serde_json::Value =
            serde_json::from_str(json).map_err(|err| DataFusionError::External(err.into()))?;

        let inferred_schema = infer_arrow_schema_from_json_value(&sample_value)?;
        let fields = inferred_schema.fields().to_vec();

        self.schema = Some(Arc::new(Schema::new(fields)));

        Ok(self)
    }

    pub fn with_encoding(&mut self, encoding: &str) -> Result<&Self> {
        self.encoding = Some(StreamEncoding::from_str(encoding)?);
        Ok(self)
    }

    // pub async fn build(&mut self) -> KafkaTopicConfig {
    //     KafkaTopicConfig {
    //
    //     }
    // }
}
