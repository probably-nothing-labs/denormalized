use std::sync::Arc;
use std::time::Duration;

use datafusion::common::Result;

use datafusion::logical_expr::builder::add_group_by_exprs_from_dependencies;
use datafusion::logical_expr::expr_rewriter::normalize_cols;
use datafusion::logical_expr::logical_plan::{Extension, LogicalPlan};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::{Aggregate, Expr};

pub mod streaming_window;
use streaming_window::{StreamingWindowPlanNode, StreamingWindowSchema, StreamingWindowType};

/// Extend the DataFusion logical plan builder with streaming specific functionality
pub trait StreamingLogicalPlanBuilder {
    fn streaming_window(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<LogicalPlanBuilder>;
}

// Extend the LogicalPlanBuilder with functions to add streaming operators to the plan
impl StreamingLogicalPlanBuilder for LogicalPlanBuilder {
    /// Apply a streaming window functions to extend the schema
    fn streaming_window(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, self.plan())?;
        let aggr_expr = normalize_cols(aggr_expr, self.plan())?;

        let group_expr = add_group_by_exprs_from_dependencies(group_expr, self.schema())?;
        let window: StreamingWindowType = slide.map_or_else(
            || StreamingWindowType::Tumbling(window_length),
            |_slide| StreamingWindowType::Sliding(window_length, _slide),
        );

        let plan = self.plan().clone();

        Aggregate::try_new(Arc::new(plan.clone()), group_expr, aggr_expr)
            .map(|new_aggr: Aggregate| {
                LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamingWindowPlanNode {
                        window_type: window,
                        window_schema: StreamingWindowSchema::try_new(new_aggr.clone()).unwrap(),
                        aggregrate: new_aggr.clone(),
                        input: plan,
                    }),
                })
            })
            .map(Self::from)
    }
}
