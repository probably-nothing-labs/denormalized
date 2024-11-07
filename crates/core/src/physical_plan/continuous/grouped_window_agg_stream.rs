use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::array::*;
use arrow::{
    compute::{concat_batches, filter_record_batch},
    datatypes::TimestampMillisecondType,
};

use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch, StructArray, TimestampMillisecondArray};
use arrow_ord::cmp;
use arrow_schema::{Schema, SchemaRef};
use crossbeam::channel::Receiver;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::{
    common::{utils::proxy::VecAllocExt, DataFusionError, Result},
    execution::memory_pool::{MemoryConsumer, MemoryReservation},
    logical_expr::EmitTo,
    physical_plan::{aggregates::PhysicalGroupBy, PhysicalExpr},
};
use datafusion::{
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::{
        aggregates::{
            aggregate_expressions,
            group_values::{new_group_values, GroupValues},
            order::GroupOrdering,
            AggregateMode,
        },
        metrics::BaselineMetrics,
    },
};

use denormalized_orchestrator::{
    channel_manager::take_receiver, orchestrator::OrchestrationMessage,
};
use futures::{executor::block_on, Stream, StreamExt};
use log::debug;
use serde::{Deserialize, Serialize};

use crate::{
    config_extensions::denormalized_config::DenormalizedConfig,
    physical_plan::utils::time::RecordBatchWatermark,
    state_backend::slatedb::{get_global_slatedb, SlateDBWrapper},
    utils::serialization::ArrayContainer,
};

use super::{
    add_window_columns_to_record_batch, add_window_columns_to_schema, create_group_accumulator,
    streaming_window::{
        get_windows_for_watermark, PhysicalStreamingWindowType, StreamingWindowExec,
    },
    GroupsAccumulatorItem,
};

#[allow(dead_code)]
pub struct GroupedWindowAggStream {
    pub schema: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    exec_aggregate_expressions: Vec<AggregateFunctionExpr>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    latest_watermark: Arc<Mutex<Option<SystemTime>>>,
    window_frames: BTreeMap<SystemTime, GroupedAggWindowFrame>,
    window_type: PhysicalStreamingWindowType,
    aggregation_mode: AggregateMode,
    group_by: PhysicalGroupBy,
    group_schema: Arc<Schema>,
    context: Arc<TaskContext>,
    checkpoint: bool,
    partition: usize,
    channel_tag: String,
    receiver: Option<Receiver<OrchestrationMessage>>,
    state_backend: Option<Arc<SlateDBWrapper>>,
}

#[derive(Serialize, Deserialize)]
pub struct CheckpointedGroupedWindowAggStream {
    partition: usize,
    watermark: Option<SystemTime>,
    frames: Vec<CheckpointedGroupedWindowFrame>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SerializedAccumulator {
    states: ArrayContainer,
    num_groups: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CheckpointedGroupedWindowFrame {
    window_start_time: SystemTime,
    window_end_time: SystemTime,
    accumulators: Vec<SerializedAccumulator>,
}

fn group_schema(schema: &Schema, group_count: usize) -> SchemaRef {
    let group_fields = schema.fields()[0..group_count].to_vec();
    Arc::new(Schema::new(group_fields))
}

#[allow(dead_code)]
impl GroupedWindowAggStream {
    pub fn new(
        exec_operator: &StreamingWindowExec,
        context: Arc<TaskContext>,
        partition: usize,
        watermark: Arc<Mutex<Option<SystemTime>>>,
        window_type: PhysicalStreamingWindowType,
        aggregation_mode: AggregateMode,
        channel_tag: Option<String>,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&exec_operator.schema);
        let agg_filter_expr = exec_operator.filter_expressions.clone();

        let baseline_metrics = BaselineMetrics::new(&exec_operator.metrics, partition);
        let input = exec_operator
            .input
            .execute(partition, Arc::clone(&context))?;

        let aggregate_expressions = aggregate_expressions(
            exec_operator.aggregate_expressions.as_slice(),
            &exec_operator.mode,
            0,
        )?;

        let filter_expressions = match exec_operator.mode {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                agg_filter_expr
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; exec_operator.aggregate_expressions.len()]
            }
        };

        let group_by = exec_operator.group_by.clone();
        let group_schema = group_schema(&agg_schema, group_by.expr().len());

        let receiver: Option<Receiver<OrchestrationMessage>> = channel_tag
            .as_ref()
            .and_then(|tag| take_receiver(tag.as_str()));

        let channel_tag: String = channel_tag.unwrap_or(String::from(""));

        let config_options = context
            .session_config()
            .options()
            .extensions
            .get::<DenormalizedConfig>();

        let checkpoint = config_options.map_or(false, |c| c.checkpoint);

        let mut serialized_state: Option<Vec<u8>> = None;
        let mut state_backend = None;
        if checkpoint {
            let backend = get_global_slatedb().unwrap();
            serialized_state = block_on(backend.get(channel_tag.as_bytes().to_vec()));
            state_backend = Some(backend);
        }

        let mut stream = Self {
            schema: agg_schema,
            input,
            baseline_metrics,
            exec_aggregate_expressions: exec_operator.aggregate_expressions.clone(),
            aggregate_expressions,
            filter_expressions,
            latest_watermark: watermark,
            window_frames: BTreeMap::new(),
            window_type,
            aggregation_mode,
            group_by,
            group_schema,
            context,
            checkpoint,
            partition,
            channel_tag,
            receiver,
            state_backend,
        };

        if serialized_state.is_some() {
            let bytes = serialized_state.unwrap();
            let state: CheckpointedGroupedWindowAggStream =
                bincode::deserialize(bytes.as_ref()).unwrap();
            let ranges: Vec<(SystemTime, SystemTime)> = state
                .frames
                .iter()
                .map(|f| (f.window_start_time, f.window_end_time))
                .collect();
            let _ = stream.ensure_window_frames_for_ranges(&ranges);
            state.frames.iter().for_each(|f| {
                let _ = stream.update_accumulators_for_frame(f.window_start_time, f);
            });
            let state_watermark = state.watermark.unwrap();
            stream.process_watermark(RecordBatchWatermark {
                min_timestamp: state_watermark,
                max_timestamp: state_watermark,
            });
            debug!(
                "successfully read the last checkpoint. partition was {} and watermark was at {:?}",
                state.partition, state.watermark
            );
        }

        Ok(stream)
    }

    pub fn output_schema_with_window(&self) -> SchemaRef {
        Arc::new(add_window_columns_to_schema(self.schema.clone()))
    }

    fn trigger_windows(&mut self) -> Result<RecordBatch, DataFusionError> {
        let mut results: Vec<RecordBatch> = Vec::new();
        let watermark_lock: std::sync::MutexGuard<'_, Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        let output_schema_with_window = self.output_schema_with_window();
        if let Some(watermark) = *watermark_lock {
            let mut window_frames_to_remove: Vec<SystemTime> = Vec::new();

            for (timestamp, frame) in self.window_frames.iter_mut() {
                if watermark >= frame.window_end_time {
                    let rb = frame.evaluate()?;
                    let result = if rb.num_rows() > 0 {
                        add_window_columns_to_record_batch(
                            rb,
                            frame.window_start_time,
                            frame.window_end_time,
                        )
                    } else {
                        RecordBatch::new_empty(output_schema_with_window.clone())
                    };

                    results.push(result);
                    window_frames_to_remove.push(*timestamp);
                }
            }

            for timestamp in window_frames_to_remove {
                self.window_frames.remove(&timestamp);
            }
        }
        concat_batches(&self.output_schema_with_window(), &results)
            .map_err(|err| DataFusionError::ArrowError(err, None))
    }

    fn process_watermark(&mut self, watermark: RecordBatchWatermark) {
        let mut watermark_lock: std::sync::MutexGuard<Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        if let Some(current_watermark) = *watermark_lock {
            if current_watermark <= watermark.min_timestamp {
                *watermark_lock = Some(watermark.min_timestamp)
            }
        } else {
            *watermark_lock = Some(watermark.min_timestamp)
        }
    }

    fn get_window_length(&mut self) -> Duration {
        match self.window_type {
            PhysicalStreamingWindowType::Session(duration) => duration,
            PhysicalStreamingWindowType::Sliding(duration, _) => duration,
            PhysicalStreamingWindowType::Tumbling(duration) => duration,
        }
    }

    fn ensure_window_frames_for_ranges(
        &mut self,
        ranges: &Vec<(SystemTime, SystemTime)>,
    ) -> Result<(), DataFusionError> {
        for (start_time, end_time) in ranges {
            self.window_frames.entry(*start_time).or_insert({
                let accumulators: Vec<_> = self
                    .exec_aggregate_expressions
                    .iter()
                    .map(|i| create_group_accumulator(&Arc::new(i.to_owned())))
                    .collect::<Result<_>>()?;
                let elapsed = start_time.elapsed().unwrap().as_millis();
                let name = format!("GroupedHashAggregateStream WindowStart[{elapsed}]");
                // Threading in Memory Reservation for now. We are currently not supporting spilling to disk.
                let reservation = MemoryConsumer::new(name)
                    .with_can_spill(false)
                    .register(self.context.memory_pool());
                let group_values = new_group_values(self.group_schema.clone())?;

                GroupedAggWindowFrame::new(
                    *start_time,
                    *end_time,
                    "canonical_timestamp".to_string(),
                    accumulators,
                    self.aggregate_expressions.clone(),
                    self.filter_expressions.clone(),
                    self.group_by.clone(),
                    self.schema.clone(),
                    self.baseline_metrics.clone(),
                    group_values,
                    Default::default(),
                    GroupOrdering::None,
                    reservation,
                )
            });
        }
        Ok(())
    }

    fn update_accumulators_for_frame(
        &mut self,
        window_start_time: SystemTime,
        state: &CheckpointedGroupedWindowFrame,
    ) -> Result<(), DataFusionError> {
        let frame = self.window_frames.get_mut(&window_start_time).unwrap();
        let _ = frame.initialize_from_state(state);
        Ok(())
    }

    #[inline]
    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let result: std::prelude::v1::Result<RecordBatch, DataFusionError> = match self
            .input
            .poll_next_unpin(cx)
        {
            Poll::Ready(rdy) => match rdy {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let watermark: RecordBatchWatermark =
                            RecordBatchWatermark::try_from(&batch, "_streaming_internal_metadata")?;
                        let ranges = get_windows_for_watermark(&watermark, self.window_type);
                        let _ = self.ensure_window_frames_for_ranges(&ranges);
                        for range in ranges {
                            let frame = self.window_frames.get_mut(&range.0).unwrap();
                            let _ = frame.push(&batch);
                        }
                        self.process_watermark(watermark);

                        self.trigger_windows()
                    } else {
                        Ok(RecordBatch::new_empty(self.output_schema_with_window()))
                    }
                }
                Some(Err(e)) => Err(e),
                None => Ok(RecordBatch::new_empty(self.output_schema_with_window())),
            },
            Poll::Pending => {
                return Poll::Pending;
            }
        };

        let mut checkpoint_batch = false;

        if self.checkpoint {
            let r = self.receiver.as_ref().unwrap();
            for message in r.try_iter() {
                debug!("received checkpoint barrier for {:?}", message);
                if let OrchestrationMessage::CheckpointBarrier(_epoch_ts) = message {
                    checkpoint_batch = true;
                }
            }

            if checkpoint_batch {
                // Prepare data for checkpointing

                // Clone or extract necessary data
                let frames: Vec<CheckpointedGroupedWindowFrame> = self
                    .window_frames
                    .values_mut()
                    .map(|frame| {
                        let num_groups = frame.group_values.len();
                        let accumulators: Vec<SerializedAccumulator> = frame
                            .accumulators
                            .iter_mut()
                            .map(|acc| {
                                let states = acc.state(EmitTo::All).unwrap();
                                SerializedAccumulator {
                                    states: ArrayContainer { arrays: states },
                                    num_groups,
                                }
                            })
                            .collect();
                        let window_start_time = frame.window_start_time;
                        let window_end_time = frame.window_end_time;
                        let checkpointed_frame = CheckpointedGroupedWindowFrame {
                            window_start_time,
                            window_end_time,
                            accumulators,
                        };
                        // accumulators get reset on .state(), so reseed them with state
                        let _ = frame.initialize_from_state(&checkpointed_frame);
                        checkpointed_frame
                    })
                    .collect();

                let watermark = {
                    let watermark_lock = self.latest_watermark.lock().unwrap();
                    *watermark_lock
                };

                let checkpointed_state = CheckpointedGroupedWindowAggStream {
                    partition: self.partition,
                    watermark,
                    frames,
                };

                let serialized_checkpoint = bincode::serialize(&checkpointed_state).unwrap();
                let key = self.channel_tag.as_bytes().to_vec();

                // Clone or use `Arc` for `state_backend`
                let state_backend = self.state_backend.clone().unwrap();

                state_backend.put(key, serialized_checkpoint);
            }
        }
        Poll::Ready(Some(result))
    }
}

impl RecordBatchStream for GroupedWindowAggStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for GroupedWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll: Poll<Option<std::prelude::v1::Result<RecordBatch, DataFusionError>>> =
            self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct GroupedAggWindowFrame {
    pub window_start_time: SystemTime,
    pub window_end_time: SystemTime,
    pub timestamp_column: String,
    pub accumulators: Vec<GroupsAccumulatorItem>,
    pub aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    pub filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    pub group_by: PhysicalGroupBy,
    /// GROUP BY expressions
    /// An interning store of group keys
    group_values: Box<dyn GroupValues>,
    /// scratch space for the current input [`RecordBatch`] being
    /// processed. Reused across batches here to avoid reallocations
    current_group_indices: Vec<usize>,

    /// Optional ordering information, that might allow groups to be
    /// emitted from the hash table prior to seeing the end of the
    /// input
    group_ordering: GroupOrdering,
    reservation: MemoryReservation,

    pub schema: SchemaRef,
    pub baseline_metrics: BaselineMetrics,
}

impl GroupedAggWindowFrame {
    pub(crate) fn new(
        window_start_time: SystemTime,
        window_end_time: SystemTime,
        timestamp_column: String,
        accumulators: Vec<GroupsAccumulatorItem>,
        aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
        group_by: PhysicalGroupBy,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
        group_values: Box<dyn GroupValues>,
        current_group_indices: Vec<usize>,
        group_ordering: GroupOrdering,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            window_start_time,
            window_end_time,
            timestamp_column,
            accumulators,
            aggregate_expressions,
            filter_expressions,
            group_by,
            group_values,
            current_group_indices,
            group_ordering,
            reservation,
            schema,
            baseline_metrics,
        }
    }

    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Evaluate the grouping expressions
        let group_by_values = evaluate_group_by(&self.group_by, &batch)?;
        // Evaluate the aggregation expressions.
        let input_values = evaluate_many(&self.aggregate_expressions, &batch)?;

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = evaluate_optional(&self.filter_expressions, &batch)?;
        for group_values in &group_by_values {
            // calculate the group indices for each input row
            let starting_num_groups = self.group_values.len();
            self.group_values
                .intern(group_values, &mut self.current_group_indices)?;
            let group_indices = &self.current_group_indices;

            // Update ordering information if necessary
            let total_num_groups = self.group_values.len();
            if total_num_groups > starting_num_groups {
                self.group_ordering
                    .new_groups(group_values, group_indices, total_num_groups)?;
            }

            // Gather the inputs to call the actual accumulator
            let t = self
                .accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            for ((acc, values), opt_filter) in t {
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());

                acc.update_batch(values, group_indices, opt_filter, total_num_groups)?;
            }
        }
        self.update_memory_reservation()
    }

    fn update_memory_reservation(&mut self) -> Result<()> {
        let acc = self.accumulators.iter().map(|x| x.size()).sum::<usize>();
        self.reservation.try_resize(
            acc + self.group_values.size()
                + self.group_ordering.size()
                + self.current_group_indices.allocated_size(),
        )
    }

    pub fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let metadata = batch
            .column_by_name("_streaming_internal_metadata")
            .unwrap();
        let metadata_struct = metadata.as_any().downcast_ref::<StructArray>().unwrap();

        let ts_column = metadata_struct
            .column_by_name("canonical_timestamp")
            .unwrap();

        let ts_array = ts_column
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap()
            .to_owned();

        let start_time_duration = self
            .window_start_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let gte_cmp_filter = cmp::gt_eq(
            &ts_array,
            &TimestampMillisecondArray::new_scalar(start_time_duration),
        )?;

        let filtered_batch: RecordBatch = filter_record_batch(batch, &gte_cmp_filter)?;

        let end_time_duration = self
            .window_end_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let metadata = filtered_batch
            .column_by_name("_streaming_internal_metadata")
            .unwrap();
        let metadata_struct = metadata.as_any().downcast_ref::<StructArray>().unwrap();

        let ts_column = metadata_struct
            .column_by_name("canonical_timestamp")
            .unwrap();

        let ts_array = ts_column
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap()
            .to_owned();

        let lt_cmp_filter = cmp::lt(
            &ts_array,
            &TimestampMillisecondArray::new_scalar(end_time_duration),
        )?;
        let final_batch = filter_record_batch(&filtered_batch, &lt_cmp_filter)?;

        let _ = self.group_aggregate_batch(final_batch);

        Ok(())
    }

    /// Create an output RecordBatch with the group keys and
    /// accumulator states/values specified in emit_to
    fn evaluate(&mut self) -> Result<RecordBatch> {
        //let timer = self.baseline_metrics.elapsed_compute().timer();

        let schema = self.schema.clone();
        if self.group_values.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        let mut output = self.group_values.emit(EmitTo::All)?;

        // Next output each aggregate value
        for acc in self.accumulators.iter_mut() {
            output.push(acc.evaluate(EmitTo::All)?)
        }

        // emit reduces the memory usage. Ignore Err from update_memory_reservation. Even if it is
        // over the target memory size after emission, we can emit again rather than returning Err.
        let _ = self.update_memory_reservation();
        let batch = RecordBatch::try_new(schema, output)?;
        Ok(batch)
    }

    fn initialize_from_state(
        &mut self,
        state: &CheckpointedGroupedWindowFrame,
    ) -> Result<(), DataFusionError> {
        let _ = self
            .accumulators
            .iter_mut()
            .zip(state.accumulators.iter())
            .map(|(acc, checkpointed_acc)| {
                let group_indices = (0..checkpointed_acc.num_groups).collect::<Vec<usize>>();
                acc.merge_batch(
                    &checkpointed_acc.states.arrays,
                    &group_indices,
                    None,
                    checkpointed_acc.num_groups,
                )
            });
        Ok(())
    }
}

pub(crate) fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    let exprs: Vec<ArrayRef> = group_by
        .expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    let null_exprs: Vec<ArrayRef> = group_by
        .null_expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(group_by
        .groups()
        .iter()
        .map(|group| {
            group
                .iter()
                .enumerate()
                .map(|(idx, is_null)| {
                    if *is_null {
                        Arc::clone(&null_exprs[idx])
                    } else {
                        Arc::clone(&exprs[idx])
                    }
                })
                .collect()
        })
        .collect())
}

/// Evaluates expressions against a record batch.
fn evaluate(expr: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|v| v.into_array(batch.num_rows()))
        })
        .collect()
}

/// Evaluates expressions against a record batch.
pub(crate) fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter().map(|expr| evaluate(expr, batch)).collect()
}

fn evaluate_optional(
    expr: &[Option<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Option<ArrayRef>>> {
    expr.iter()
        .map(|expr| {
            expr.as_ref()
                .map(|expr| {
                    expr.evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .transpose()
        })
        .collect()
}
