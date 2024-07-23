use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion_physical_plan::time::TimestampUnit;
use rdkafka::producer::FutureProducer;

use crate::arrow_helpers::infer_arrow_schema_from_json_value;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::Expr;

use rdkafka::consumer::StreamConsumer;
use rdkafka::ClientConfig;

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

    pub consumer_opts: ConnectionOpts,
    pub producer_opts: ConnectionOpts,
}

impl KafkaTopicConfig {
    pub fn make_consumer(&self) -> Result<StreamConsumer> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", self.bootstrap_servers.to_string())
            .set("enable.auto.commit", "false");

        for (key, value) in self.consumer_opts.clone().into_iter() {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config.create().expect("Consumer creation failed");
        Ok(consumer)
    }

    pub fn make_producer(&self) -> Result<FutureProducer> {
        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", self.bootstrap_servers.to_string());

        for (key, value) in self.producer_opts.clone().into_iter() {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config.create().expect("Consumer creation failed");
        Ok(producer)
    }
}

pub struct KafkaTopicConfigBuilder {
    bootstrap_servers: String,
    topic: String,

    schema: Option<SchemaRef>,
    infer_schema: bool,

    timestamp_column: Option<String>,
    timestamp_unit: Option<TimestampUnit>,

    encoding: Option<StreamEncoding>,
    consumer_opts: ConnectionOpts,
    producer_opts: ConnectionOpts,
}

impl KafkaTopicConfigBuilder {
    pub fn new(bootstrap_servers: String, topic: String) -> Self {
        Self {
            bootstrap_servers,
            topic,

            schema: None,
            infer_schema: false,

            timestamp_column: None,
            timestamp_unit: None,

            encoding: None,
            consumer_opts: ConnectionOpts::new(),
            producer_opts: ConnectionOpts::new(),
        }
    }

    pub fn with_schema(&mut self, schema: SchemaRef) -> &mut Self {
        self.infer_schema = false;
        self.schema = Some(schema);
        self
    }

    pub fn infer_schema(&mut self) -> &mut Self {
        self.infer_schema = true;
        self
    }

    pub fn infer_schema_from_json(&mut self, json: &str) -> Result<&mut Self> {
        self.infer_schema = false;

        let sample_value: serde_json::Value =
            serde_json::from_str(json).map_err(|err| DataFusionError::External(err.into()))?;

        let inferred_schema = infer_arrow_schema_from_json_value(&sample_value)?;
        let fields = inferred_schema.fields().to_vec();

        self.schema = Some(Arc::new(Schema::new(fields)));

        Ok(self)
    }

    pub fn with_timestamp(
        &mut self,
        timestamp_column: String,
        timestamp_unit: TimestampUnit,
    ) -> &mut Self {
        self.timestamp_column = Some(timestamp_column);
        self.timestamp_unit = Some(timestamp_unit);
        self
    }

    pub fn with_consumer_opts(&mut self, opts: ConnectionOpts) -> &mut Self {
        for (key, value) in opts.into_iter() {
            self.consumer_opts.insert(key.clone(), value.clone());
        }
        self
    }

    pub fn with_producer_opts(&mut self, opts: ConnectionOpts) -> &mut Self {
        for (key, value) in opts.into_iter() {
            self.producer_opts.insert(key.clone(), value.clone());
        }
        self
    }

    pub fn with_encoding(&mut self, encoding: &str) -> Result<&mut Self> {
        self.encoding = Some(StreamEncoding::from_str(encoding)?);
        Ok(self)
    }

    fn create_canonical_schema(&self) -> Result<SchemaRef> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| create_error("Schema required"))?
            .clone();

        let mut fields = schema.fields().to_vec();

        // Add a new column to the dataset that should mirror the occurred_at_ms field
        let struct_fields = vec![
            Arc::new(Field::new("barrier_batch", DataType::Utf8, false)),
            Arc::new(Field::new(
                String::from("canonical_timestamp"),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        ];
        fields.insert(
            fields.len(),
            Arc::new(Field::new(
                String::from("_streaming_internal_metadata"),
                DataType::Struct(Fields::from(struct_fields)),
                true,
            )),
        );

        Ok(Arc::new(Schema::new(fields)))
    }

    pub async fn build(&self) -> Result<KafkaTopicConfig> {
        if self.infer_schema {
            //@todo - query the kafka topic for a few messages and try to infer the schema
        }

        let original_schema = self
            .schema
            .as_ref()
            .ok_or_else(|| create_error("Schema required"))?
            .clone();

        let canonical_schema = self.create_canonical_schema()?;

        let encoding = self
            .encoding
            .as_ref()
            .ok_or_else(|| create_error("encoding required"))?
            .clone();

        let timestamp_column = self
            .timestamp_column
            .as_ref()
            .ok_or_else(|| create_error("timestamp_column required"))?
            .clone();

        let timestamp_unit = self
            .timestamp_unit
            .as_ref()
            .ok_or_else(|| create_error("timestamp_unit required"))?
            .clone();

        let consumer_opts = self.consumer_opts.clone();
        let producer_opts = self.producer_opts.clone();

        //@todo
        let order = vec![];

        //@todo - query kafka topic for number of partitions and set it here
        let partitions = 1_i32;

        Ok(KafkaTopicConfig {
            topic: self.topic.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),

            original_schema,
            schema: canonical_schema,
            encoding,
            order,
            partitions,

            timestamp_unit,
            timestamp_column,

            consumer_opts,
            producer_opts,
        })
    }
}

/// The data encoding for [`StreamTable`]
#[derive(Debug, Clone, Copy)]
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

fn create_error(msg: &str) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg,
    )))
}
