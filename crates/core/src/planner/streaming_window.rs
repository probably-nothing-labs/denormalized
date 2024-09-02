use async_trait::async_trait;
use itertools::multiunzip;
use std::sync::Arc;

use datafusion::common::{internal_err, DFSchema};
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

use crate::logical_plan::streaming_window::{StreamingWindowPlanNode, StreamingWindowType};
use crate::physical_plan::continuous::streaming_window::{
    PhysicalStreamingWindowType, StreamingWindowExec,
};
use datafusion::error::Result;

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
                let physical_input_schema = input_exec.schema();

                let logical_input_schema = logical_input.schema();

                let groups = create_grouping_physical_expr(
                    streaming_window_node.aggregrate.group_expr.as_ref(),
                    logical_input_schema,
                    _session_state,
                )?;

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
                        PhysicalStreamingWindowType::Tumbling(length)
                    }
                    StreamingWindowType::Sliding(length, slide) => {
                        PhysicalStreamingWindowType::Sliding(length, slide)
                    }
                    StreamingWindowType::Session(..) => todo!(),
                };

                let final_aggr = if streaming_window_node.aggregrate.group_expr.is_empty() {
                    let partial_aggr = Arc::new(StreamingWindowExec::try_new(
                        AggregateMode::Partial,
                        groups.clone(),
                        aggregates.clone(),
                        filters.clone(),
                        input_exec.clone(),
                        physical_input_schema.clone(),
                        franz_window_type,
                        None,
                    )?);
                    Arc::new(StreamingWindowExec::try_new(
                        AggregateMode::Final,
                        groups.clone(),
                        aggregates.clone(),
                        filters.clone(),
                        partial_aggr.clone(),
                        physical_input_schema.clone(),
                        franz_window_type,
                        None,
                    )?)
                } else {
                    Arc::new(StreamingWindowExec::try_new(
                        AggregateMode::Single,
                        groups.clone(),
                        aggregates.clone(),
                        filters.clone(),
                        input_exec.clone(),
                        physical_input_schema.clone(),
                        franz_window_type,
                        None,
                    )?)
                };

                Some(final_aggr)
            } else {
                None
            },
        )
    }
}
