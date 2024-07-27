use async_trait::async_trait;
use std::sync::Arc;
use itertools::multiunzip;

use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{
    LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateMode;


use crate::physical_plan::streaming_window::FranzStreamingWindowExec;
use crate::logical_plan::streaming_window::{StreamingWindowType, StreamingWindowPlanNode};

/// Physical planner for TopK nodes
pub struct StreamingWindowPlanner {}

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
        Ok(
            if let Some(streaming_window_node) = node.as_any().downcast_ref::<StreamingWindowPlanNode>() {
                // Initially need to perform the aggregate and then merge the partitions
                let input_exec = children.one()?; // Should be derivable from physical_inputs
                let physical_input_schema: Arc<Schema> = input_exec.schema();

                let logical_input_schema = input.as_ref().schema();

                let groups = self.create_grouping_physical_expr(
                    group_expr,
                    logical_input_schema,
                    &physical_input_schema,
                    session_state,
                )?;

                let agg_filter = aggr_expr
                    .iter()
                    .map(|e| {
                        create_aggregate_expr_and_maybe_filter(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let (aggregates, filters, _order_bys): (Vec<_>, Vec<_>, Vec<_>) =
                    multiunzip(agg_filter);
                let franz_window_type = match window_type {
                    StreamingWindowType::Tumbling(length) => {
                        StreamingWindowType::Tumbling(length.clone())
                    }
                    StreamingWindowType::Sliding(length, slide) => {
                        StreamingWindowType::Sliding(length.clone(), slide.clone())
                    }
                    StreamingWindowType::Session(length, key) => todo!(),
                };
                let initial_aggr = Arc::new(FranzStreamingWindowExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates.clone(),
                    filters.clone(),
                    input_exec,
                    physical_input_schema.clone(),
                    franz_window_type,
                )?);
                initial_aggr
            } else {
                None
            },
        )
    }
}
