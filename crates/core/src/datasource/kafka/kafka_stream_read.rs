use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::TimestampMillisecondType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use denormalized_orchestrator::channel_manager::{create_channel, get_sender};
use denormalized_orchestrator::orchestrator::{self, OrchestrationMessage};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::Value;
//use tracing::{debug, error};

use crate::config_extensions::denormalized_config::DenormalizedConfig;
use crate::physical_plan::stream_table::PartitionStreamExt;
use crate::physical_plan::utils::time::array_to_timestamp_array;
use crate::state_backend::rocksdb_backend::get_global_rocksdb;
use crate::utils::arrow_helpers::json_records_to_arrow_record_batch;

use arrow::compute::{max, min};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::streaming::PartitionStream;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Timestamp, TopicPartitionList};

use super::KafkaReadConfig;

#[derive(Clone)]
pub struct KafkaStreamRead {
    pub config: Arc<KafkaReadConfig>,
    pub assigned_partitions: Vec<i32>,
    pub exec_node_id: Option<usize>,
}

impl KafkaStreamRead {
    pub fn with_node_id(self, node_id: Option<usize>) -> KafkaStreamRead {
        Self {
            config: self.config.clone(),
            assigned_partitions: self.assigned_partitions.clone(),
            exec_node_id: node_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchReadMetadata {
    epoch: i32,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    offsets_read: HashMap<i32, i64>,
}

impl BatchReadMetadata {
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

fn create_consumer(config: Arc<KafkaReadConfig>) -> StreamConsumer {
    let mut client_config = ClientConfig::new();

    client_config
        .set("bootstrap.servers", config.bootstrap_servers.to_string())
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");

    for (key, value) in config.kafka_connection_opts.clone().into_iter() {
        client_config.set(key, value);
    }

    client_config.create().expect("Consumer creation failed")
}

impl PartitionStream for KafkaStreamRead {
    fn schema(&self) -> &SchemaRef {
        &self.config.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut assigned_partitions = TopicPartitionList::new();

        let config_options = ctx
            .session_config()
            .options()
            .extensions
            .get::<DenormalizedConfig>();

        let should_checkpoint = config_options.map_or(false, |c| c.checkpoint);

        let topic = self.config.topic.clone();
        for partition in self.assigned_partitions.clone() {
            assigned_partitions.add_partition(self.config.topic.as_str(), partition);
        }
        let partition_tag = self
            .assigned_partitions
            .iter()
            .map(|&x| x.to_string())
            .collect::<Vec<String>>()
            .join("_");

        let state_backend = if should_checkpoint {
            Some(get_global_rocksdb().unwrap())
        } else {
            None
        };
        let consumer: StreamConsumer = create_consumer(self.config.clone());

        consumer
            .assign(&assigned_partitions)
            .expect("Partition assignment failed.");

        let state_namespace = format!("kafka_source_{}", topic);

        if let Some(backend) = &state_backend {
            let _ = match backend.get_cf(&state_namespace) {
                Ok(cf) => {
                    debug!("cf for this already exists");
                    Ok(cf)
                }
                Err(..) => {
                    let _ = backend.create_cf(&state_namespace);
                    backend.get_cf(&state_namespace)
                }
            };
        }

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.config.schema.clone(), 1);
        let tx = builder.tx();
        let canonical_schema = self.config.schema.clone();
        let json_schema = self.config.original_schema.clone();
        let timestamp_column: String = self.config.timestamp_column.clone();
        let timestamp_unit = self.config.timestamp_unit.clone();
        let batch_timeout = Duration::from_millis(100);
        let mut channel_tag = String::from("");
        if orchestrator::SHOULD_CHECKPOINT {
            let node_id = self.exec_node_id.unwrap();
            channel_tag = format!("{}_{}", node_id, partition_tag);
            create_channel(channel_tag.as_str(), 10);
        }
        builder.spawn(async move {
            let mut epoch = 0;
            if orchestrator::SHOULD_CHECKPOINT {
                let orchestrator_sender = get_sender("orchestrator");
                let msg = OrchestrationMessage::RegisterStream(channel_tag.clone());
                orchestrator_sender.as_ref().unwrap().send(msg).unwrap();
            }
            loop {
                let mut last_offsets = HashMap::new();
                if let Some(backend) = &state_backend {
                    if let Some(offsets) = backend
                        .get_state(&state_namespace, partition_tag.clone().into_bytes())
                        .unwrap()
                    {
                        let last_batch_metadata = BatchReadMetadata::from_bytes(&offsets).unwrap();
                        last_offsets = last_batch_metadata.offsets_read;
                        debug!(
                            "epoch is {} and last read offsets are {:?}",
                            epoch, last_offsets
                        );
                    } else {
                        debug!("epoch is {} and no prior offsets were found.", epoch);
                    }
                }

                for (partition, offset) in &last_offsets {
                    consumer
                        .seek(
                            &topic,
                            *partition,
                            rdkafka::Offset::Offset(*offset + 1),
                            Duration::from_secs(10),
                        )
                        .expect("Failed to seek to stored offset");
                }

                let mut offsets_read: HashMap<i32, i64> = HashMap::new();
                let mut batch: Vec<serde_json::Value> = Vec::new();
                let start_time = datafusion::common::instant::Instant::now();

                while start_time.elapsed() < batch_timeout {
                    match tokio::time::timeout(
                        batch_timeout - start_time.elapsed(),
                        consumer.recv(),
                    )
                    .await
                    {
                        Ok(Ok(m)) => {
                            let timestamp = match m.timestamp() {
                                Timestamp::NotAvailable => -1_i64,
                                Timestamp::CreateTime(ts) => ts,
                                Timestamp::LogAppendTime(ts) => ts,
                            };
                            let key = m.key();

                            let payload = m.payload().expect("Message payload is empty");
                            let mut deserialized_record: HashMap<String, Value> =
                                serde_json::from_slice(payload).unwrap();
                            deserialized_record
                                .insert("kafka_timestamp".to_string(), Value::from(timestamp));
                            if let Some(key) = key {
                                deserialized_record.insert(
                                    "kafka_key".to_string(),
                                    Value::from(String::from_utf8_lossy(key)),
                                );
                            } else {
                                deserialized_record
                                    .insert("kafka_key".to_string(), Value::from(String::from("")));
                            }
                            let new_payload = serde_json::to_value(deserialized_record).unwrap();
                            offsets_read.insert(m.partition(), m.offset());
                            batch.push(new_payload);
                        }
                        Ok(Err(err)) => {
                            error!("Error reading from Kafka {:?}", err);
                            // TODO: Implement a retry mechanism here
                        }
                        Err(_) => {
                            // Timeout reached
                            break;
                        }
                    }
                }

                //debug!("Batch size {}", batch.len());

                if !batch.is_empty() {
                    let record_batch: RecordBatch =
                        json_records_to_arrow_record_batch(batch, json_schema.clone());

                    let ts_column = record_batch
                        .column_by_name(timestamp_column.as_str())
                        .map(|ts_col| {
                            Arc::new(array_to_timestamp_array(ts_col, timestamp_unit.clone()))
                        })
                        .unwrap();

                    let binary_vec = Vec::from_iter(
                        std::iter::repeat(String::from("no_barrier")).take(ts_column.len()),
                    );
                    let barrier_batch_column = StringArray::from(binary_vec);

                    let ts_array = ts_column
                        .as_any()
                        .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                        .unwrap();

                    let max_timestamp: Option<_> = max::<TimestampMillisecondType>(ts_array);
                    let min_timestamp: Option<_> = min::<TimestampMillisecondType>(ts_array);
                    debug!("min: {:?}, max: {:?}", min_timestamp, max_timestamp);
                    let mut columns: Vec<Arc<dyn Array>> = record_batch.columns().to_vec();

                    let metadata_column = StructArray::from(vec![
                        (
                            Arc::new(Field::new("barrier_batch", DataType::Utf8, false)),
                            Arc::new(barrier_batch_column) as ArrayRef,
                        ),
                        (
                            Arc::new(Field::new(
                                "canonical_timestamp",
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                true,
                            )),
                            ts_column as ArrayRef,
                        ),
                    ]);
                    columns.push(Arc::new(metadata_column));

                    let timestamped_record_batch: RecordBatch =
                        RecordBatch::try_new(canonical_schema.clone(), columns).unwrap();
                    let tx_result = tx.send(Ok(timestamped_record_batch)).await;
                    match tx_result {
                        Ok(_) => {
                            if should_checkpoint {
                                let _ = state_backend.as_ref().map(|backend| {
                                    backend.put_state(
                                        &state_namespace,
                                        partition_tag.clone().into_bytes(),
                                        BatchReadMetadata {
                                            epoch,
                                            min_timestamp,
                                            max_timestamp,
                                            offsets_read,
                                        }
                                        .to_bytes()
                                        .unwrap(),
                                    )
                                });
                            }
                        }
                        Err(err) => error!("result err {:?}", err),
                    }
                    epoch += 1;
                }
            }
        });
        builder.build()
    }
}

// Implement this for KafkaStreamRead
impl PartitionStreamExt for KafkaStreamRead {
    fn requires_node_id(&self) -> bool {
        true
    }

    fn as_partition_with_node_id(&self) -> Option<&KafkaStreamRead> {
        Some(self)
    }
}
