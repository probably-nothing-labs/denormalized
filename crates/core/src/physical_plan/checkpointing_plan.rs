use std::sync::Arc;

use datafusion::{
    common::tree_node::TreeNodeVisitor, error::DataFusionError, physical_plan::ExecutionPlan,
};

struct PlanVistor {
    sources: Vec<Option<usize>>,
}


pub trait CheckpointingPlan: ExecutionPlan {
    fn upstream_sources(&self, input: Arc<dyn ExecutionPlan>) -> Vec<Option<usize>>;

    fn register_checkpoint_epoch(&self, source: usize, epoch: String);
}

pub trait Checkpoint {
    fn checkpoint(&self) -> Result<(), DataFusionError> {
        Ok(())
    }
}
