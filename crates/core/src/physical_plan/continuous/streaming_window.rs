use std::{
    any::Any,
    borrow::Cow,
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::{
    compute::{concat_batches, filter_record_batch, max},
    datatypes::TimestampMillisecondType,
};
use arrow_array::{Array, PrimitiveArray, RecordBatch, StructArray, TimestampMillisecondArray};
use arrow_ord::cmp;
use arrow_schema::{Field, Schema, SchemaRef};

use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{
    equivalence::{collapse_lex_req, ProjectionMapping},
    expressions::UnKnownColumn,
    Partitioning, PhysicalExpr, PhysicalSortRequirement,
};
use datafusion::physical_plan::{
    aggregates::{
        aggregate_expressions, finalize_aggregation, get_finer_aggregate_exprs_requirement,
        AggregateMode, PhysicalGroupBy,
    },
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    windows::get_ordered_partition_by_indices,
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
    InputOrderMode, PlanProperties,
};

use datafusion::{
    common::{internal_err, stats::Precision, DataFusionError, Statistics},
    physical_plan::Distribution,
};
use denormalized_orchestrator::{
    channel_manager::{create_channel, get_sender},
    orchestrator::OrchestrationMessage,
};
use futures::{Stream, StreamExt};
use tracing::debug;

use crate::{
    config_extensions::denormalized_config::DenormalizedConfig,
    physical_plan::{
        continuous::grouped_window_agg_stream::GroupedWindowAggStream,
        utils::{
            accumulators::{create_accumulators, AccumulatorItem},
            time::{system_time_from_epoch, RecordBatchWatermark},
        },
    },
};

pub struct PartialWindowAggFrame {
    pub window_start_time: SystemTime,
    window_end_time: SystemTime,
    timestamp_column: String,
    accumulators: Vec<AccumulatorItem>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    aggregation_mode: AggregateMode,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl DisplayAs for PartialWindowAggFrame {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WindowAggExec: ")?;
                write!(
                    f,
                    "start_time: {:?}, end_time {:?}, timestamp_column {}",
                    self.window_start_time.duration_since(UNIX_EPOCH),
                    self.window_end_time.duration_since(UNIX_EPOCH),
                    self.timestamp_column
                )?;
            }
        }
        Ok(())
    }
}

use datafusion::common::Result;

use super::{add_window_columns_to_record_batch, add_window_columns_to_schema, batch_filter};

impl PartialWindowAggFrame {
    pub fn new(
        window_start_time: SystemTime,
        window_end_time: SystemTime,
        timestamp_column: String,
        accumulators: Vec<AccumulatorItem>,
        aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
        aggregation_mode: AggregateMode,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            window_start_time,
            window_end_time,
            timestamp_column,
            accumulators,
            aggregate_expressions,
            filter_expressions,
            aggregation_mode,
            schema,
            baseline_metrics,
        }
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

        let _ = aggregate_batch(
            &self.aggregation_mode,
            final_batch,
            &mut self.accumulators,
            &self.aggregate_expressions,
            &self.filter_expressions,
        );
        Ok(())
    }

    pub fn evaluate(&mut self) -> Result<RecordBatch, DataFusionError> {
        let timer = self.baseline_metrics.elapsed_compute().timer();
        let result = finalize_aggregation(&mut self.accumulators, &self.aggregation_mode).and_then(
            |columns| RecordBatch::try_new(self.schema.clone(), columns).map_err(Into::into),
        );

        timer.done();
        result
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PhysicalStreamingWindowType {
    Session(Duration),
    Sliding(Duration, Duration),
    Tumbling(Duration),
}

#[derive(Debug)]
pub struct StreamingWindowExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub aggregate_expressions: Vec<AggregateFunctionExpr>,
    pub filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Schema after the window is run
    pub group_by: PhysicalGroupBy,
    pub schema: SchemaRef,
    pub input_schema: SchemaRef,

    pub watermark: Arc<Mutex<Option<SystemTime>>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
    pub mode: AggregateMode,
    pub window_type: PhysicalStreamingWindowType,
    pub upstream_partitioning: Option<usize>,
}

impl StreamingWindowExec {
    /// Create a new execution plan for window aggregates
    ///
    pub fn try_new(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<AggregateFunctionExpr>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        window_type: PhysicalStreamingWindowType,
        upstream_partitioning: Option<usize>,
    ) -> Result<Self> {
        let schema = create_schema(
            &input.schema(),
            group_by.expr(),
            &aggr_expr,
            false, //group_by.contains_null(),
            mode,
        )?;

        let schema = Arc::new(schema);
        StreamingWindowExec::try_new_with_schema(
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            input_schema,
            schema,
            window_type,
            upstream_partitioning,
        )
    }

    pub fn try_new_with_schema(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        mut aggr_expr: Vec<AggregateFunctionExpr>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        schema: SchemaRef,
        window_type: PhysicalStreamingWindowType,
        upstream_partitioning: Option<usize>,
    ) -> Result<Self> {
        if aggr_expr.len() != filter_expr.len() {
            return internal_err!("Inconsistent aggregate expr: {:?} and filter expr: {:?} for AggregateExec, their size should match", aggr_expr, filter_expr);
        }

        let input_eq_properties = input.equivalence_properties();
        // Get GROUP BY expressions:
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of the GROUP BY expressions,
        // prefix requirements with this section. In this case, aggregation will
        // work more efficiently.
        let indices = get_ordered_partition_by_indices(&groupby_exprs, &input);
        let mut _new_requirement = indices
            .iter()
            .map(|&idx| PhysicalSortRequirement {
                expr: groupby_exprs[idx].clone(),
                options: None,
            })
            .collect::<Vec<_>>();

        let req = get_finer_aggregate_exprs_requirement(
            &mut aggr_expr,
            &group_by,
            input_eq_properties,
            &mode,
        )?;
        _new_requirement.extend(req);
        _new_requirement = collapse_lex_req(_new_requirement);

        let input_order_mode = if indices.len() == groupby_exprs.len() && !indices.is_empty() {
            InputOrderMode::Sorted
        } else if !indices.is_empty() {
            InputOrderMode::PartiallySorted(indices)
        } else {
            InputOrderMode::Linear
        };

        // construct a map from the input expression to the output expression of the Aggregation group by
        let projection_mapping = ProjectionMapping::try_new(group_by.expr(), &input.schema())?;

        let cache = StreamingWindowExec::compute_properties(
            &input,
            Arc::new(add_window_columns_to_schema(schema.clone())),
            &projection_mapping,
            &mode,
            &input_order_mode,
        );

        Ok(Self {
            input,
            aggregate_expressions: aggr_expr,
            filter_expressions: filter_expr,
            group_by,
            schema,
            input_schema,
            watermark: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            mode,
            window_type,
            upstream_partitioning,
        })
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    pub fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        projection_mapping: &ProjectionMapping,
        mode: &AggregateMode,
        _input_order_mode: &InputOrderMode,
    ) -> PlanProperties {
        // Construct equivalence properties:
        let eq_properties = input
            .equivalence_properties()
            .project(projection_mapping, schema);

        // Get output partitioning:
        let mut output_partitioning = input.output_partitioning().clone();
        if mode.is_first_stage() {
            // First stage aggregation will not change the output partitioning,
            // but needs to respect aliases (e.g. mapping in the GROUP BY
            // expression).
            let input_eq_properties = input.equivalence_properties();
            if let Partitioning::Hash(exprs, part) = output_partitioning {
                let normalized_exprs = exprs
                    .iter()
                    .map(|expr| {
                        input_eq_properties
                            .project_expr(expr, projection_mapping)
                            .unwrap_or_else(|| Arc::new(UnKnownColumn::new(&expr.to_string())))
                    })
                    .collect();
                output_partitioning = Partitioning::Hash(normalized_exprs, part);
            }
        }

        PlanProperties::new(eq_properties, output_partitioning, ExecutionMode::Unbounded)
    }
    /// Aggregate expressions
    pub fn aggr_expr(&self) -> &[AggregateFunctionExpr] {
        &self.aggregate_expressions
    }

    /// Grouping expressions as they occur in the output schema
    pub fn output_group_expr(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.group_by.output_exprs()
    }
}

impl ExecutionPlan for StreamingWindowExec {
    fn name(&self) -> &'static str {
        "StreamingWindowExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingWindowExec::try_new(
            self.mode,
            self.group_by.clone(),
            self.aggregate_expressions.clone(),
            self.filter_expressions.clone(),
            children[0].clone(),
            self.input_schema.clone(),
            self.window_type,
            self.upstream_partitioning,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let node_id = self
            .properties()
            .node_id()
            .expect("expected node id to be set.");

        let config_options = context
            .session_config()
            .options()
            .extensions
            .get::<DenormalizedConfig>();

        let checkpoint = config_options.map_or(false, |c| c.checkpoint);

        let channel_tag = if checkpoint {
            let tag = format!("{}_{}", node_id, partition);
            create_channel(tag.as_str(), 10);
            let orchestrator_sender = get_sender("orchestrator");
            let msg: OrchestrationMessage = OrchestrationMessage::RegisterStream(tag.clone());
            orchestrator_sender.as_ref().unwrap().send(msg).unwrap();
            Some(tag)
        } else {
            None
        };

        if self.group_by.is_empty() {
            debug!("GROUP BY expression is empty creating a SimpleWindowAggStream");
            if self.mode == AggregateMode::Partial {
                Ok(Box::pin(WindowAggStream::new(
                    self,
                    context,
                    partition,
                    self.watermark.clone(),
                    self.window_type,
                    self.mode,
                    channel_tag,
                )?))
            } else {
                Ok(Box::pin(FullWindowAggStream::try_new(
                    self,
                    context,
                    partition,
                    Duration::from_millis(100),
                )?))
            }
        } else {
            debug!("Creating a GroupedWindowAggStream");
            Ok(Box::pin(GroupedWindowAggStream::new(
                self,
                context,
                partition,
                self.watermark.clone(),
                self.window_type,
                self.mode,
                channel_tag,
            )?))
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.mode == AggregateMode::Final {
            return vec![Distribution::SinglePartition; self.children().len()];
        }
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let column_statistics = Statistics::unknown_column(&self.schema());
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned if self.group_by.is_empty() => {
                Ok(Statistics {
                    num_rows: Precision::Exact(1),
                    column_statistics,
                    total_byte_size: Precision::Absent,
                })
            }
            _ => {
                // When the input row count is 0 or 1, we can adopt that statistic keeping its reliability.
                // When it is larger than 1, we degrade the precision since it may decrease after aggregation.
                let num_rows = if let Some(value) = self.input().statistics()?.num_rows.get_value()
                {
                    if *value > 1 {
                        self.input().statistics()?.num_rows.to_inexact()
                    } else if *value == 0 {
                        // Aggregation on an empty table creates a null row.
                        self.input()
                            .statistics()?
                            .num_rows
                            .add(&Precision::Exact(1))
                    } else {
                        // num_rows = 1 case
                        self.input().statistics()?.num_rows
                    }
                } else {
                    Precision::Absent
                };
                Ok(Statistics {
                    num_rows,
                    column_statistics,
                    total_byte_size: Precision::Absent,
                })
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(add_window_columns_to_schema(self.schema.clone()))
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn with_node_id(self: Arc<Self>, _node_id: usize) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let cache = self.properties().clone().with_node_id(_node_id);
        let new_exec = StreamingWindowExec {
            input: self.input.clone(),
            aggregate_expressions: self.aggregate_expressions.clone(),
            filter_expressions: self.filter_expressions.clone(),
            group_by: self.group_by.clone(),
            schema: self.schema.clone(),
            input_schema: self.input_schema.clone(),
            watermark: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            mode: self.mode,
            window_type: self.window_type,
            upstream_partitioning: self.upstream_partitioning,
        };
        Ok(Some(Arc::new(new_exec)))
    }
}

impl DisplayAs for StreamingWindowExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StreamingWindowExec: mode={:?}", self.mode)?;
                let g: Vec<String> = if self.group_by.is_single() {
                    self.group_by
                        .expr()
                        .iter()
                        .map(|(e, alias)| {
                            let e = e.to_string();
                            if &e != alias {
                                format!("{e} as {alias}")
                            } else {
                                e
                            }
                        })
                        .collect()
                } else {
                    self.group_by
                        .groups()
                        .iter()
                        .map(|group| {
                            let terms = group
                                .iter()
                                .enumerate()
                                .map(|(idx, is_null)| {
                                    if *is_null {
                                        let (e, alias) = &self.group_by.null_expr()[idx];
                                        let e = e.to_string();
                                        if e != *alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    } else {
                                        let (e, alias) = &self.group_by.expr()[idx];
                                        let e = e.to_string();
                                        if e != *alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    }
                                })
                                .collect::<Vec<String>>()
                                .join(", ");
                            format!("({terms})")
                        })
                        .collect()
                };

                write!(f, ", gby=[{}]", g.join(", "))?;

                let a: Vec<String> = self
                    .aggregate_expressions
                    .iter()
                    .map(|agg| agg.name().to_string())
                    .collect();
                write!(f, ", aggr=[{}]", a.join(", "))?;
                write!(f, ", window_type=[{:?}]", self.window_type)?;
                //if let Some(limit) = self.limit {
                //    write!(f, ", lim=[{limit}]")?;
                //}

                //if self.input_order_mode != InputOrderMode::Linear {
                //    write!(f, ", ordering_mode={:?}", self.input_order_mode)?;
                //}
            }
        }
        Ok(())
    }
}

pub struct WindowAggStream {
    pub schema: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    exec_aggregate_expressions: Vec<AggregateFunctionExpr>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    latest_watermark: Arc<Mutex<Option<SystemTime>>>,
    window_frames: BTreeMap<SystemTime, PartialWindowAggFrame>,
    window_type: PhysicalStreamingWindowType,
    aggregation_mode: AggregateMode,
    _channel_tag: Option<String>,
}

#[allow(dead_code)]
impl WindowAggStream {
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

        let aggregate_expressions =
            aggregate_expressions(&exec_operator.aggregate_expressions, &exec_operator.mode, 0)?;
        let filter_expressions = match exec_operator.mode {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                agg_filter_expr
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; exec_operator.aggregate_expressions.len()]
            }
        };

        Ok(Self {
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
            _channel_tag: channel_tag,
        })
    }

    pub fn output_schema_with_window(&self) -> SchemaRef {
        Arc::new(add_window_columns_to_schema(self.schema.clone()))
    }

    fn trigger_windows(&mut self) -> Result<RecordBatch, DataFusionError> {
        let mut results: Vec<RecordBatch> = Vec::new();
        let watermark_lock: std::sync::MutexGuard<'_, Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        if let Some(watermark) = *watermark_lock {
            let mut window_frames_to_remove: Vec<SystemTime> = Vec::new();

            for (timestamp, frame) in self.window_frames.iter_mut() {
                if watermark >= frame.window_end_time {
                    let rb = frame.evaluate()?;
                    let result = add_window_columns_to_record_batch(
                        rb,
                        frame.window_start_time,
                        frame.window_end_time,
                    );
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
        // should this be within a mutex?
        let mut watermark_lock: std::sync::MutexGuard<Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        debug!("latest watermark currently is {:?}", *watermark_lock);
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
                let accumulators = create_accumulators(&self.exec_aggregate_expressions)?;
                PartialWindowAggFrame::new(
                    *start_time,
                    *end_time,
                    "canonical_timestamp".to_string(),
                    accumulators,
                    self.aggregate_expressions.clone(),
                    self.filter_expressions.clone(),
                    self.aggregation_mode,
                    self.schema.clone(),
                    self.baseline_metrics.clone(),
                )
            });
        }
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
        Poll::Ready(Some(result))
    }
}

impl RecordBatchStream for WindowAggStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for WindowAggStream {
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

struct FullWindowAggFrame {
    window_start_time: SystemTime,
    window_end_time: SystemTime,
    accumulators: Vec<AccumulatorItem>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    schema: SchemaRef,
    _baseline_metrics: BaselineMetrics,
    batches_accumulated: usize,
}

impl FullWindowAggFrame {
    pub fn new(
        start_time: SystemTime,
        end_time: SystemTime,
        exec_aggregate_expressions: &[AggregateFunctionExpr],
        aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let accumulators = create_accumulators(exec_aggregate_expressions).unwrap();
        Self {
            window_start_time: start_time,
            window_end_time: end_time,
            accumulators,
            aggregate_expressions,
            filter_expressions,
            schema: schema.clone(),
            _baseline_metrics: baseline_metrics,
            batches_accumulated: 0,
        }
    }

    fn aggregate_batch(&mut self, batch: RecordBatch) {
        let _ = aggregate_batch(
            &AggregateMode::Final,
            batch,
            &mut self.accumulators,
            &self.aggregate_expressions,
            &self.filter_expressions,
        );
        self.batches_accumulated += 1;
    }

    fn evaluate(&mut self) -> Result<RecordBatch, DataFusionError> {
        finalize_aggregation(&mut self.accumulators, &AggregateMode::Final).and_then(|columns| {
            RecordBatch::try_new(self.schema.clone(), columns).map_err(Into::into)
        })
    }
}
struct FullWindowAggStream {
    pub schema: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    exec_aggregate_expressions: Vec<AggregateFunctionExpr>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    cached_frames: BTreeMap<SystemTime, FullWindowAggFrame>,
    watermark: Option<SystemTime>, // This stream needs to be run with only one partition in the Exec operator.
    _lateness_threshold: Duration,
    seen_windows: std::collections::HashSet<SystemTime>,
}

impl FullWindowAggStream {
    pub fn try_new(
        exec_operator: &StreamingWindowExec,
        context: Arc<TaskContext>,
        partition: usize,
        lateness_threshold: Duration,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&exec_operator.schema);
        let baseline_metrics = BaselineMetrics::new(&exec_operator.metrics, partition);
        let input = exec_operator
            .input
            .execute(partition, Arc::clone(&context))?;

        let aggregate_expressions =
            aggregate_expressions(&exec_operator.aggregate_expressions, &exec_operator.mode, 0)?;
        let filter_expressions = vec![None; exec_operator.aggregate_expressions.len()];
        let exec_aggregate_expressions = exec_operator.aggregate_expressions.clone();
        Ok(Self {
            schema: agg_schema,
            input,
            baseline_metrics,
            exec_aggregate_expressions,
            aggregate_expressions,
            filter_expressions,
            cached_frames: BTreeMap::new(),
            watermark: None,
            _lateness_threshold: lateness_threshold,
            seen_windows: std::collections::HashSet::<SystemTime>::new(),
        })
    }

    fn finalize_windows(&mut self) -> Result<RecordBatch, DataFusionError> {
        let mut results: Vec<RecordBatch> = Vec::new();
        let mut frame_to_remove: Vec<SystemTime> = Vec::new();
        for (timestamp, frame) in self.cached_frames.iter_mut() {
            //TODO: May need to add this frame.batches_accumulated == self.upstream_partitions
            if self
                .watermark
                .map_or_else(|| false, |ts| ts > frame.window_end_time)
            {
                let rb = frame.evaluate()?;
                let result = add_window_columns_to_record_batch(
                    rb,
                    frame.window_start_time,
                    frame.window_end_time,
                );
                results.push(result);
                frame_to_remove.push(*timestamp);
            }
        }
        for timestamp in frame_to_remove {
            self.cached_frames.remove(&timestamp);
        }
        concat_batches(
            &Arc::new(add_window_columns_to_schema(self.schema.clone())),
            &results,
        )
        .map_err(|err| DataFusionError::ArrowError(err, None))
    }

    #[inline]
    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let result = match self.input.poll_next_unpin(cx) {
            Poll::Ready(rdy) => match rdy {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let mut rb = batch;
                        let batch_start_time = rb.column_by_name("window_start_time").map(|col| {
                            let timestamp = col
                                .as_any()
                                .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                                .unwrap();
                            system_time_from_epoch(
                                max::<TimestampMillisecondType>(timestamp).unwrap() as _, // these batches should only have 1 row per batch
                            )
                        });
                        let batch_end_time = rb.column_by_name("window_end_time").map(|col| {
                            let timestamp = col
                                .as_any()
                                .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                                .unwrap();
                            system_time_from_epoch(
                                max::<TimestampMillisecondType>(timestamp).unwrap() as _, // these batches should only have 1 row per batch
                            )
                        });
                        let start_time = batch_start_time.unwrap(); //TODO: Remove unwrap.

                        if self.seen_windows.contains(&start_time)
                            && !self.cached_frames.contains_key(&start_time)
                        {
                            panic!("we are reopening a window already seen.")
                        }
                        let frame = self.cached_frames.entry(start_time).or_insert({
                            FullWindowAggFrame::new(
                                start_time,
                                batch_end_time.unwrap(),
                                &self.exec_aggregate_expressions,
                                self.aggregate_expressions.clone(),
                                self.filter_expressions.clone(),
                                self.schema.clone(),
                                self.baseline_metrics.clone(),
                            )
                        });

                        self.seen_windows.insert(start_time);

                        //last two columns are timestamp columns, so remove them before pushing them onto a frame.
                        let col_size = rb.num_columns();
                        rb.remove_column(col_size - 1);
                        rb.remove_column(col_size - 2);

                        frame.aggregate_batch(rb);

                        self.watermark = self
                            .watermark
                            .map_or(Some(start_time), |w| Some(w.max(start_time)));

                        self.finalize_windows()
                    } else {
                        Ok(RecordBatch::new_empty(Arc::new(
                            add_window_columns_to_schema(self.schema.clone()),
                        )))
                    }
                }
                Some(Err(e)) => Err(e),
                None => Ok(RecordBatch::new_empty(Arc::new(
                    add_window_columns_to_schema(self.schema.clone()),
                ))),
            },
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(Some(result))
    }
}

impl RecordBatchStream for FullWindowAggStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for FullWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll: Poll<Option<std::prelude::v1::Result<RecordBatch, DataFusionError>>> =
            self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

pub fn get_windows_for_watermark(
    watermark: &RecordBatchWatermark,
    window_type: PhysicalStreamingWindowType,
) -> Vec<(SystemTime, SystemTime)> {
    let start_time = watermark.min_timestamp;
    let end_time = watermark.max_timestamp;
    let mut window_ranges = Vec::new();

    match window_type {
        PhysicalStreamingWindowType::Session(_) => todo!(),
        PhysicalStreamingWindowType::Sliding(window_length, slide) => {
            let mut current_start = snap_to_window_start(start_time - window_length, window_length);
            while current_start <= end_time {
                let current_end = current_start + window_length;
                if start_time > current_end || end_time < current_start {
                    // out of bounds
                    current_start += slide;
                    continue;
                }
                window_ranges.push((current_start, current_end));
                current_start += slide;
            }
        }
        PhysicalStreamingWindowType::Tumbling(window_length) => {
            let mut current_start: SystemTime = snap_to_window_start(start_time, window_length);
            while current_start <= end_time {
                let current_end = current_start + window_length;
                window_ranges.push((current_start, current_end));
                current_start = current_end;
            }
        }
    };
    window_ranges
}

fn snap_to_window_start(timestamp: SystemTime, window_length: Duration) -> SystemTime {
    let timestamp_duration = timestamp.duration_since(UNIX_EPOCH).unwrap();
    let window_length_secs = window_length.as_secs();
    let timestamp_secs = timestamp_duration.as_secs();
    let window_start_secs = (timestamp_secs / window_length_secs) * window_length_secs;
    UNIX_EPOCH + Duration::from_secs(window_start_secs)
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[AggregateFunctionExpr],
    contains_null_expr: bool,
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            // In cases where we have multiple grouping sets, we will use NULL expressions in
            // order to align the grouping sets. So the field must be nullable even if the underlying
            // schema field is not.
            contains_null_expr || expr.nullable(input_schema)?,
        ))
    }

    match mode {
        AggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned())
            }
        }
        AggregateMode::Final
        | AggregateMode::FinalPartitioned
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field())
            }
        }
    }

    Ok(Schema::new(fields))
}

pub fn aggregate_batch(
    mode: &AggregateMode,
    batch: RecordBatch,
    accumulators: &mut [AccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
    filters: &[Option<Arc<dyn PhysicalExpr>>],
) -> Result<usize> {
    let mut allocated = 0usize;

    // 1.1 iterate accumulators and respective expressions together
    // 1.2 filter the batch if necessary
    // 1.3 evaluate expressions
    // 1.4 update / merge accumulators with the expressions' values

    // 1.1
    accumulators
        .iter_mut()
        .zip(expressions)
        .zip(filters)
        .try_for_each(|((accum, expr), filter)| {
            // 1.2
            let batch = match filter {
                Some(filter) => Cow::Owned(batch_filter(&batch, filter)?),
                None => Cow::Borrowed(&batch),
            };
            // 1.3
            let values = &expr
                .iter()
                .map(|e| {
                    e.evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;

            // 1.4
            let size_pre = accum.size();
            let res = match mode {
                AggregateMode::Partial
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned => accum.update_batch(values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => accum.merge_batch(values),
            };
            let size_post = accum.size();
            allocated += size_post.saturating_sub(size_pre);
            res
        })?;

    Ok(allocated)
}
