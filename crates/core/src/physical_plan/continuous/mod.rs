use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::PrimitiveBuilder, compute::filter_record_batch, datatypes::TimestampMillisecondType,
};
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use datafusion::{
    common::{downcast_value, DataFusionError, Result},
    logical_expr::GroupsAccumulator,
    physical_expr::GroupsAccumulatorAdapter,
    physical_plan::PhysicalExpr,
};
pub mod grouped_window_agg_stream;
pub mod streaming_window;

use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use log::debug;

pub(crate) type GroupsAccumulatorItem = Box<dyn GroupsAccumulator>;

pub(crate) fn create_group_accumulator(
    agg_expr: &Arc<AggregateFunctionExpr>,
) -> Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        // Note in the log when the slow path is used
        debug!(
            "Creating GroupsAccumulatorAdapter for {}: {agg_expr:?}",
            agg_expr.name()
        );
        let agg_expr_captured = Arc::clone(agg_expr);
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}

fn add_window_columns_to_schema(schema: SchemaRef) -> Schema {
    let fields = schema.fields();

    let mut builder = SchemaBuilder::new();

    for field in fields {
        builder.push(field.clone());
    }
    builder.push(Field::new(
        "window_start_time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));
    builder.push(Field::new(
        "window_end_time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));

    builder.finish()
}

fn add_window_columns_to_record_batch(
    record_batch: RecordBatch,
    start_time: SystemTime,
    end_time: SystemTime,
) -> RecordBatch {
    let start_time_duration = start_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let end_time_duration = end_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

    let mut start_builder = PrimitiveBuilder::<TimestampMillisecondType>::new();
    let mut end_builder = PrimitiveBuilder::<TimestampMillisecondType>::new();

    for _ in 0..record_batch.num_rows() {
        start_builder.append_value(start_time_duration);
        end_builder.append_value(end_time_duration);
    }

    let start_array = start_builder.finish();
    let end_array = end_builder.finish();

    let new_schema = add_window_columns_to_schema(record_batch.schema());
    let mut new_columns = record_batch.columns().to_vec();
    new_columns.push(Arc::new(start_array));
    new_columns.push(Arc::new(end_array));

    RecordBatch::try_new(Arc::new(new_schema), new_columns).unwrap()
}

pub fn as_boolean_array(array: &dyn Array) -> Result<&BooleanArray> {
    Ok(downcast_value!(array, BooleanArray))
}

fn batch_filter(batch: &RecordBatch, predicate: &Arc<dyn PhysicalExpr>) -> Result<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(batch, filter_array)?))
        })
}
