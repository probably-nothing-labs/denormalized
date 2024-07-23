use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::TimestampMillisecondType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use datafusion_common::franz_arrow::json_records_to_arrow_record_batch;
use futures::StreamExt;
use serde_json::Value;
use tracing::{debug, error, info, instrument};

use arrow::compute::{max, min};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::time::array_to_timestamp_array;
use rdkafka::consumer::Consumer;
use rdkafka::{Message, Timestamp, TopicPartitionList};

use super::KafkaTopicConfig;

pub struct KafkaStreamRead {
    pub config: Arc<KafkaTopicConfig>,
    pub assigned_partitions: Vec<i32>,
}

impl PartitionStream for KafkaStreamRead {
    fn schema(&self) -> &SchemaRef {
        &self.config.schema
    }

    #[instrument(name = "KafkaStreamRead::execute", skip(self, _ctx))]
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut assigned_partitions = TopicPartitionList::new();
        for partition in self.assigned_partitions.clone() {
            assigned_partitions.add_partition(self.config.topic.as_str(), partition);
        }
        info!("Reading partition {:?}", assigned_partitions);

        let consumer = self
            .config
            .make_consumer()
            .expect("Consumer creation failed");

        consumer
            .assign(&assigned_partitions)
            .expect("Partition assignment failed.");

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.config.schema.clone(), 1);
        let tx = builder.tx();
        let canonical_schema = self.config.schema.clone();
        let json_schema = self.config.original_schema.clone();
        let timestamp_column: String = self.config.timestamp_column.clone();
        let timestamp_unit = self.config.timestamp_unit.clone();

        let _ = builder.spawn(async move {
            loop {
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
                    Ok(m) => debug!("result ok {:?}", m),
                    Err(err) => error!("result err {:?}", err),
                }
            }
        });
        builder.build()
    }
}
