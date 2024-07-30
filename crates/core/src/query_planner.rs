use async_trait::async_trait;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

use crate::planner::streaming_window::StreamingWindowPlanner;
pub struct StreamingQueryPlanner {}

#[async_trait]
impl QueryPlanner for StreamingQueryPlanner {
    /// Given a `LogicalPlan`, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            StreamingWindowPlanner {},
        )]);

        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
