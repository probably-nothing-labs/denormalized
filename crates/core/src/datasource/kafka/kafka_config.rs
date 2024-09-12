use std::collections::HashMap;
use std::str::FromStr;
use std::{sync::Arc, time::Duration};

use apache_avro::Schema as AvroSchema;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use datafusion::logical_expr::SortExpr;

use crate::formats::decoders::avro::AvroDecoder;
use crate::formats::decoders::json::JsonDecoder;
use crate::formats::decoders::utils::to_arrow_schema;
use crate::formats::decoders::Decoder;
use crate::formats::StreamEncoding;
use crate::physical_plan::utils::time::TimestampUnit;
use crate::utils::arrow_helpers::infer_arrow_schema_from_json_value;
use denormalized_common::error::{DenormalizedError, Result};

use super::{TopicReader, TopicWriter};

use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

pub type ConnectionOpts = HashMap<String, String>;

/// The configuration for a [`StreamTable`]
#[derive(Debug)]
pub struct KafkaReadConfig {
    pub topic: String,
    pub bootstrap_servers: String,

    pub original_schema: SchemaRef,
    pub schema: SchemaRef,

    pub encoding: StreamEncoding,
    pub order: Vec<Vec<SortExpr>>,
    pub partition_count: i32,
    pub timestamp_column: String,
    pub timestamp_unit: TimestampUnit,

    pub kafka_connection_opts: ConnectionOpts,
}

impl KafkaReadConfig {
    pub fn make_consumer(&self) -> Result<StreamConsumer> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", self.bootstrap_servers.to_string())
            .set("enable.auto.commit", "false");

        for (key, value) in self.kafka_connection_opts.clone().into_iter() {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config.create().expect("Consumer creation failed");
        Ok(consumer)
    }

    pub fn build_decoder(&self) -> Box<dyn Decoder> {
        match self.encoding {
            StreamEncoding::Avro => Box::new(AvroDecoder::new(self.original_schema.clone())),
            StreamEncoding::Json => Box::new(JsonDecoder::new(self.original_schema.clone())),
        }
    }
}

#[derive(Debug)]
pub struct KafkaWriteConfig {
    pub topic: String,
    pub bootstrap_servers: String,

    pub schema: SchemaRef,

    pub encoding: StreamEncoding,
    pub partition_count: i32,
    pub timestamp_column: String,
    pub timestamp_unit: TimestampUnit,

    pub kafka_connection_opts: ConnectionOpts,
}

impl KafkaWriteConfig {
    pub fn make_producer(&self) -> Result<FutureProducer> {
        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", self.bootstrap_servers.to_string());
        client_config.set("message.timeout.ms", "60000");

        for (key, value) in self.kafka_connection_opts.clone().into_iter() {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config.create().expect("Consumer creation failed");
        Ok(producer)
    }
}

#[derive(Debug, Clone)]
pub struct KafkaTopicBuilder {
    bootstrap_servers: String,
    topic: Option<String>,

    schema: Option<SchemaRef>,
    infer_schema: bool,

    timestamp_column: Option<String>,
    timestamp_unit: Option<TimestampUnit>,

    encoding: Option<StreamEncoding>,
}

impl KafkaTopicBuilder {
    pub fn new(bootstrap_servers: String) -> Self {
        Self {
            bootstrap_servers,
            topic: None,

            schema: None,
            infer_schema: false,

            timestamp_column: None,
            timestamp_unit: None,

            encoding: None,
        }
    }

    pub fn with_topic(&mut self, topic: String) -> &mut Self {
        self.topic = Some(topic);
        self
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
            serde_json::from_str(json).map_err(|err| DenormalizedError::Other(err.into()))?;

        let inferred_schema = infer_arrow_schema_from_json_value(&sample_value)?;
        let fields = inferred_schema.fields().to_vec();

        self.schema = Some(Arc::new(Schema::new(fields)));

        Ok(self)
    }

    pub fn infer_schema_from_avro(&mut self, avro_schema_str: &str) -> Result<&mut Self> {
        self.infer_schema = false;
        let avro_schema: AvroSchema =
            AvroSchema::parse_str(avro_schema_str).expect("Invalid schema!");
        let arrow_schema = to_arrow_schema(&avro_schema)?;
        self.schema = Some(Arc::new(arrow_schema));
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

    pub fn with_encoding(&mut self, encoding: &str) -> Result<&mut Self> {
        self.encoding = Some(StreamEncoding::from_str(encoding)?);
        Ok(self)
    }

    fn create_canonical_schema(&self) -> Result<SchemaRef> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("Schema required".to_string()))?
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

    pub async fn build_reader(&self, opts: ConnectionOpts) -> Result<TopicReader> {
        let topic = self
            .topic
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("topic required".to_string()))?
            .clone();

        let original_schema = self
            .schema
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("Schema required".to_string()))?
            .clone();

        let canonical_schema = self.create_canonical_schema()?;

        let encoding = *self
            .encoding
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("encoding required".to_string()))?;

        let timestamp_column = self
            .timestamp_column
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("timestamp_column required".to_string()))?
            .clone();

        let timestamp_unit = self
            .timestamp_unit
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("timestamp_unit required".to_string()))?
            .clone();

        let mut kafka_connection_opts = ConnectionOpts::new();
        for (key, value) in opts.into_iter() {
            kafka_connection_opts.insert(key.clone(), value.clone());
        }

        //@todo
        let order = vec![];

        let partition_count =
            get_topic_partition_count(self.bootstrap_servers.clone(), topic.clone())?;

        let config = KafkaReadConfig {
            topic,
            bootstrap_servers: self.bootstrap_servers.clone(),

            original_schema,
            schema: canonical_schema,
            encoding,
            order,
            partition_count,

            timestamp_unit,
            timestamp_column,

            kafka_connection_opts,
        };

        Ok(TopicReader(Arc::new(config)))
    }

    pub async fn build_writer(&self, opts: ConnectionOpts) -> Result<TopicWriter> {
        let topic = self
            .topic
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("topic required".to_string()))?
            .clone();

        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("Schema required".to_string()))?
            .clone();

        let encoding = *self
            .encoding
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("encoding required".to_string()))?;

        let timestamp_column = self
            .timestamp_column
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("timestamp_column required".to_string()))?
            .clone();

        let timestamp_unit = self
            .timestamp_unit
            .as_ref()
            .ok_or_else(|| DenormalizedError::KafkaConfig("timestamp_unit required".to_string()))?
            .clone();

        let mut kafka_connection_opts = ConnectionOpts::new();
        for (key, value) in opts.into_iter() {
            kafka_connection_opts.insert(key.clone(), value.clone());
        }

        let partition_count =
            get_topic_partition_count(self.bootstrap_servers.clone(), topic.clone())?;

        let config = KafkaWriteConfig {
            topic,
            bootstrap_servers: self.bootstrap_servers.clone(),

            schema,
            encoding,
            partition_count,

            timestamp_unit,
            timestamp_column,
            kafka_connection_opts,
        };

        Ok(TopicWriter(Arc::new(config)))
    }
}

fn get_topic_partition_count(bootstrap_servers: String, topic: String) -> Result<i32> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", bootstrap_servers.to_string());

    let consumer: StreamConsumer = client_config.create().expect("Consumer creation failed");

    let data = consumer
        .fetch_metadata(Some(topic.as_str()), Duration::from_millis(5_000))
        .unwrap();
    let topic_metadata = data.topics();
    let md = &topic_metadata[0];
    let partitions = md.partitions();

    Ok(partitions.len() as i32)
}
