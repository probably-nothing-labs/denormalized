use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::TimestampMillisecondType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use datafusion_expr::Expr;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error, info, instrument};

use crate::state_backend::rocksdb_backend::get_global_rocksdb;
use crate::utils::arrow_helpers::json_records_to_arrow_record_batch;
use arrow::compute::{max, min};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::time::array_to_timestamp_array;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Timestamp, TopicPartitionList};

use super::{KafkaReadConfig, StreamEncoding};

pub struct KafkaStreamRead {
    pub config: Arc<KafkaReadConfig>,
    pub assigned_partitions: Vec<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchReadMetadata {
    epoch: i32,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    offsets_read: Vec<(i32, i64)>,
}

impl BatchReadMetadata {
    // Serialize to Vec<u8> using bincode
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    // Deserialize from Vec<u8> using bincode
    fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

fn create_consumer(config: Arc<KafkaReadConfig>) -> StreamConsumer {
    let mut client_config = ClientConfig::new();

    client_config
        .set("bootstrap.servers", config.bootstrap_servers.to_string())
        .set("enable.auto.commit", "false");

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

        let state_backend = get_global_rocksdb().unwrap();
        let consumer: StreamConsumer = create_consumer(self.config.clone());

        consumer
            .assign(&assigned_partitions)
            .expect("Partition assignment failed.");

        let state_namespace = format!("kafka_source_{}", topic);

        let _ = match state_backend.get_cf(&state_namespace) {
            Ok(cf) => {
                debug!("cf for this already exists");
                Ok(cf)
            }
            Err(..) => {
                let _ = state_backend.create_cf(&state_namespace);
                state_backend.get_cf(&state_namespace)
            }
        };
        //let schema = self.config.schema.clone();

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.config.schema.clone(), 1);
        let tx = builder.tx();
        let canonical_schema = self.config.schema.clone();
        let json_schema = self.config.original_schema.clone();
        let timestamp_column: String = self.config.timestamp_column.clone();
        let timestamp_unit = self.config.timestamp_unit.clone();

        let _ = state_backend.create_cf(state_namespace.as_str());
        let _ = builder.spawn(async move {
            let mut epoch = 0;
            loop {
                let last_read_offsets = state_backend
                    .get_state(&state_namespace, partition_tag.clone().into_bytes())
                    .await?;

                match last_read_offsets {
                    Some(offsets) => {
                        let last_batch_metadata = BatchReadMetadata::from_bytes(&offsets).unwrap();
                        debug!(
                            "epoch is {} and last read offsets are {:?}",
                            epoch, last_batch_metadata
                        );
                    }
                    None => debug!("epoch is {} and no prior offsets were found.", epoch),
                };
                let mut offsets_read: Vec<(i32, i64)> = vec![];
                let batch: Vec<serde_json::Value> = consumer
                    .stream()
                    .take_until(tokio::time::sleep(Duration::from_secs(1)))
                    .map(|message| match message {
                        Ok(m) => {
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
                            offsets_read.push((m.partition(), m.offset()));
                            new_payload
                        }
                        Err(err) => {
                            error!("Error reading from Kafka {:?}", err);
                            panic!("Error reading from Kafka {:?}", err)
                        }
                    })
                    .collect()
                    .await;

                debug!("Batch size {}", batch.len());

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

                let max_timestamp: Option<_> = max::<TimestampMillisecondType>(&ts_array);
                let min_timestamp: Option<_> = min::<TimestampMillisecondType>(&ts_array);
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
                    Ok(m) => {
                        let _ = state_backend
                            .put_state(
                                &state_namespace,
                                partition_tag.clone().into_bytes(),
                                BatchReadMetadata {
                                    epoch,
                                    min_timestamp,
                                    max_timestamp,
                                    offsets_read,
                                }
                                .to_bytes()
                                .unwrap(), //TODO: Fix the error threading.
                            )
                            .await;
                    }
                    Err(err) => error!("result err {:?}", err),
                }
                epoch += 1;
            }
        });
        builder.build()
    }
}
