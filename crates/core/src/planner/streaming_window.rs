use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use async_trait::async_trait;
use itertools::multiunzip;
use std::sync::Arc;

use datafusion::common::{internal_err, Column, DFSchema};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::{
    aggregates::{AggregateMode, PhysicalGroupBy},
    ExecutionPlan,
};
use datafusion::physical_planner::{
    create_aggregate_expr_and_maybe_filter, ExtensionPlanner, PhysicalPlanner,
};

use crate::logical_plan::streaming_window::{
    StreamingWindowPlanNode, StreamingWindowSchema, StreamingWindowType,
};
use crate::physical_plan::streaming_window::{
    add_window_columns_to_physical_schema, FranzStreamingWindowExec, FranzStreamingWindowType,
};

/// Physical planner for TopK nodes
pub struct StreamingWindowPlanner {}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

fn create_grouping_physical_expr(
    group_expr: &[Expr],
    input_dfschema: &DFSchema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    if group_expr.len() == 1 {
        let expr = &group_expr[0];
        match expr {
            Expr::Column(c) => Ok(PhysicalGroupBy::new_single(vec![tuple_err((
                create_physical_expr(expr, input_dfschema, session_state.execution_props()),
                Ok(c.name.clone()),
            ))?])),
            _ => internal_err!("Expected a column expression in GROUP BY for Streaming Window"),
        }
    } else {
        Ok(PhysicalGroupBy::new_single(
            group_expr
                .iter()
                .map(|e| match e {
                    Expr::Column(c) => tuple_err((
                        create_physical_expr(e, input_dfschema, session_state.execution_props()),
                        Ok(c.flat_name()),
                    )),
                    _ => internal_err!(
                        "Expected a column expression in GROUP BY for Streaming Window"
                    ),
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

fn add_window_columns(inner_schema: SchemaRef) -> DFSchema {
    let fields = inner_schema.fields().to_owned();

    let mut builder = SchemaBuilder::new();

    for field in fields.iter() {
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
    DFSchema::try_from(builder.finish()).unwrap()
}

#[async_trait]
impl ExtensionPlanner for StreamingWindowPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        assert_eq!(
            logical_inputs.len(),
            1,
            "Inconsistent number of logical inputs. A Streaming Window should have only 1 input."
        );
        assert_eq!(
            physical_inputs.len(),
            1,
            "Inconsistent number of physical inputs. A Streaming Window should have only 1 input."
        );
        Ok(
            if let Some(streaming_window_node) =
                node.as_any().downcast_ref::<StreamingWindowPlanNode>()
            {
                // Initially need to perform the aggregate and then merge the partitions

                let logical_input = logical_inputs[0];
                let input_exec = &physical_inputs[0]; // Should be derivable from physical_inputs
                let physical_input_schema =
                    add_window_columns_to_physical_schema(input_exec.schema());
                assert_eq!(
                    physical_input_schema.fields().len(),
                    7,
                    "physical input should have 7 fields."
                );
                let logical_input_schema = logical_input.schema();

                let schema_for_group_exprs = DFSchema::try_from(physical_input_schema.clone())?;
                println!(
                    "new columns !!!!!! {:?}\n",
                    schema_for_group_exprs.columns().len()
                );
                let window_start_column = Column::new_unqualified("window_start_time");
                let window_end_column = Column::new_unqualified("window_end_time");
                let new_columns = vec![
                    Expr::Column(window_start_column),
                    Expr::Column(window_end_column),
                ];
                let mut _group_expr_with_windows =
                    streaming_window_node.aggregrate.group_expr.clone();
                _group_expr_with_windows.extend(new_columns);

                let groups = create_grouping_physical_expr(
                    _group_expr_with_windows.as_ref(),
                    &schema_for_group_exprs,
                    _session_state,
                )?;

                println!(
                    ">>>>>>>> aggr expressions = {:?} \n group expressions = {:?}",
                    streaming_window_node.aggregrate.aggr_expr, groups
                );
                let agg_filter = streaming_window_node
                    .aggregrate
                    .aggr_expr
                    .iter()
                    .map(|e| {
                        create_aggregate_expr_and_maybe_filter(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            _session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let (aggregates, filters, _order_bys): (Vec<_>, Vec<_>, Vec<_>) =
                    multiunzip(agg_filter);
                let franz_window_type = match streaming_window_node.window_type {
                    StreamingWindowType::Tumbling(length) => {
                        FranzStreamingWindowType::Tumbling(length)
                    }
                    StreamingWindowType::Sliding(length, slide) => {
                        FranzStreamingWindowType::Sliding(length, slide)
                    }
                    StreamingWindowType::Session(..) => todo!(),
                };

                let initial_aggr = Arc::new(FranzStreamingWindowExec::try_new(
                    AggregateMode::Single,
                    groups.clone(),
                    aggregates.clone(),
                    filters.clone(),
                    input_exec.clone(),
                    input_exec.schema(),
                    franz_window_type,
                )?);
                println!(">>>> SUCCESSS <<<<<");
                Some(initial_aggr)
            } else {
                None
            },
        )
    }
}
