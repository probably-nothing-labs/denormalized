use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::TimestampMillisecondType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use crossbeam::channel;
use denormalized_orchestrator::channel_manager::{create_channel, get_sender, take_receiver};
use denormalized_orchestrator::orchestrator::OrchestrationMessage;
use futures::executor::block_on;
use log::{debug, error};
use serde::{Deserialize, Serialize};

use crate::config_extensions::denormalized_config::DenormalizedConfig;
use crate::physical_plan::stream_table::PartitionStreamExt;
use crate::physical_plan::utils::time::array_to_timestamp_array;
use crate::state_backend::slatedb::get_global_slatedb;

use arrow::compute::{max, min};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::streaming::PartitionStream;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

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
    epoch: u128,
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
        let config_options = ctx
            .session_config()
            .options()
            .extensions
            .get::<DenormalizedConfig>();

        let should_checkpoint = config_options.map_or(false, |c| c.checkpoint);

        let node_id = self.exec_node_id.unwrap();
        let partition_tag = self
            .assigned_partitions
            .iter()
            .map(|&x| x.to_string())
            .collect::<Vec<String>>()
            .join("_");

        let channel_tag = format!("{}_{}", node_id, partition_tag);
        let mut serialized_state: Option<Vec<u8>> = None;
        let mut state_backend = None;

        let mut starting_offsets: HashMap<i32, i64> = HashMap::new();

        if should_checkpoint {
            create_channel(channel_tag.as_str(), 10);
            let backend = get_global_slatedb().unwrap();
            debug!("checking for last checkpointed offsets");
            serialized_state = block_on(backend.get(channel_tag.as_bytes().to_vec()));
            state_backend = Some(backend);
        }

        if let Some(serialized_state) = serialized_state {
            let last_batch_metadata = BatchReadMetadata::from_bytes(&serialized_state).unwrap();
            debug!(
                "recovering from checkpointed offsets. epoch was {} max timestamp {:?}",
                last_batch_metadata.epoch, last_batch_metadata.max_timestamp
            );
            starting_offsets = last_batch_metadata.offsets_read.clone();
        }

        let mut assigned_partitions = TopicPartitionList::new();

        for partition in self.assigned_partitions.clone() {
            assigned_partitions.add_partition(self.config.topic.as_str(), partition);
            if starting_offsets.contains_key(&partition) {
                let offset = starting_offsets.get(&partition).unwrap();
                debug!("setting partition {} to offset {}", partition, offset);
                let _ = assigned_partitions.set_partition_offset(
                    self.config.topic.as_str(),
                    partition,
                    Offset::from_raw(*offset),
                );
            }
        }

        let consumer: StreamConsumer = create_consumer(self.config.clone());

        consumer
            .assign(&assigned_partitions)
            .expect("Partition assignment failed.");

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.config.schema.clone(), 1);
        let tx = builder.tx();
        let canonical_schema = self.config.schema.clone();
        let timestamp_column: String = self.config.timestamp_column.clone();
        let timestamp_unit = self.config.timestamp_unit.clone();
        let batch_timeout: Duration = Duration::from_millis(100);
        let mut decoder = self.config.build_decoder();

        builder.spawn(async move {
            let mut epoch = 0;
            let mut receiver: Option<channel::Receiver<OrchestrationMessage>> = None;
            if should_checkpoint {
                let orchestrator_sender = get_sender("orchestrator");
                let msg: OrchestrationMessage =
                    OrchestrationMessage::RegisterStream(channel_tag.clone());
                orchestrator_sender.as_ref().unwrap().send(msg).unwrap();
                receiver = take_receiver(channel_tag.as_str());
            }
            let mut checkpoint_batch = false;

            loop {
                //let mut checkpoint_barrier: Option<String> = None;
                let mut _checkpoint_barrier: Option<i64> = None;

                if should_checkpoint {
                    let r = receiver.as_ref().unwrap();
                    for message in r.try_iter() {
                        debug!("received checkpoint barrier for {:?}", message);
                        if let OrchestrationMessage::CheckpointBarrier(epoch_ts) = message {
                            epoch = epoch_ts;
                            checkpoint_batch = true;
                        }
                    }
                }

                let mut offsets_read: HashMap<i32, i64> = HashMap::new();

                loop {
                    match tokio::time::timeout(batch_timeout, consumer.recv()).await {
                        Ok(Ok(m)) => {
                            let payload = m.payload().expect("Message payload is empty");
                            decoder.push_to_buffer(payload.to_owned());
                            offsets_read
                                .entry(m.partition())
                                .and_modify(|existing_value| {
                                    *existing_value = (*existing_value).max(m.offset())
                                })
                                .or_insert(m.offset());
                            break;
                        }
                        Ok(Err(err)) => {
                            error!("Error reading from Kafka {:?}", err);
                            // TODO: Implement a retry mechanism here
                        }
                        Err(_) => {
                            // Timeout reached
                            error!("timeout reached.");
                            //break;
                        }
                    }
                }

                if !offsets_read.is_empty() {
                    let record_batch = decoder.to_record_batch().unwrap();
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
                            if checkpoint_batch {
                                debug!("about to checkpoint offsets");
                                let off = BatchReadMetadata {
                                    epoch,
                                    min_timestamp,
                                    max_timestamp,
                                    offsets_read,
                                };
                                state_backend
                                    .as_ref()
                                    .unwrap()
                                    .put(channel_tag.as_bytes().to_vec(), off.to_bytes().unwrap());
                                debug!("checkpointed offsets {:?}", off);
                                checkpoint_batch = false;
                            }
                        }
                        Err(err) => error!("result err {:?}. shutdown signal detected.", err),
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
