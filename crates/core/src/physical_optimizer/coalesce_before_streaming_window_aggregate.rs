use std::sync::Arc;

use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlanProperties;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;

use crate::physical_plan::continuous::streaming_window::StreamingWindowExec;

pub struct EnsureHashPartititionOnGroupByForStreamingAggregates {}

impl Default for EnsureHashPartititionOnGroupByForStreamingAggregates {
    fn default() -> Self {
        Self::new()
    }
}

impl EnsureHashPartititionOnGroupByForStreamingAggregates {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

#[allow(unused_variables)]
// Franz optimizer rule, added to ensure coalescing of partitions before a global aggregate
// window. This rule may be removed once we have support for two stage partial and final
// aggregates a la vanilla Datafusion.
impl PhysicalOptimizerRule for EnsureHashPartititionOnGroupByForStreamingAggregates {
    fn optimize(
        &self,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        config: &datafusion::common::config::ConfigOptions,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        plan.transform(|original| {
            if let Some(streaming_aggr_exec) =
                original.as_any().downcast_ref::<StreamingWindowExec>()
            {
                let input = streaming_aggr_exec.input();
                let partitions = match input.output_partitioning() {
                    datafusion::physical_expr::Partitioning::RoundRobinBatch(size) => size,
                    datafusion::physical_expr::Partitioning::Hash(_, size) => size,
                    datafusion::physical_expr::Partitioning::UnknownPartitioning(size) => size,
                };
                if *partitions == 1 {
                    return Ok(Transformed::no(original));
                }
                let group_by = streaming_aggr_exec.group_by.expr();
                // Make sure we dont have contiguous repartitions. TODO: Make this a separate rule.
                let input_exec =
                    if let Some(in_exec) = input.as_any().downcast_ref::<RepartitionExec>() {
                        in_exec.input()
                    } else {
                        input
                    };

                let coalesce_exec = if streaming_aggr_exec.group_by.is_empty() {
                    return Ok(Transformed::no(original));
                } else {
                    Arc::new(RepartitionExec::try_new(
                        input_exec.clone(),
                        Partitioning::Hash(
                            streaming_aggr_exec.group_by.input_exprs(),
                            input.output_partitioning().partition_count(),
                        ),
                    )?)
                };
                Ok(Transformed::yes(Arc::new(StreamingWindowExec::try_new(
                    streaming_aggr_exec.mode,
                    streaming_aggr_exec.group_by.clone(),
                    streaming_aggr_exec.aggregate_expressions.clone(),
                    streaming_aggr_exec.filter_expressions.clone(),
                    coalesce_exec.clone(),
                    input.schema(),
                    streaming_aggr_exec.window_type,
                    streaming_aggr_exec.upstream_partitioning,
                )?)))
            } else {
                Ok(Transformed::no(original))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "coalesce_before_streaming_aggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// #[cfg(test)]
// pub(crate) mod tests {
//     use std::{sync::Arc, time::Duration};
//
//     use datafusion::common::config::ConfigOptions;
//     use datafusion::physical_plan::displayable;
//
//     use datafusion::physical_plan::memory::MemoryExec;
//     use datafusion::{error::Result, execution::context::SessionContext};
//     use arrow::datatypes::{DataType, Field, Schema};
//     use datafusion_physical_plan::ExecutionPlan;
//
//     use datafusion::physical_plan::aggregates::PhysicalGroupBy;
//     use arrow_array::{Int32Array, RecordBatch};
//     use datafusion_physical_plan::{
//         aggregates::AggregateMode,
//         continuous::window::{FranzStreamingWindowExec, FranzStreamingWindowType},
//     };
//
//     use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
//
//     // @todo TestAggregate is private to datafusion, can we make it public?
//     use datafusion::physical_optimizer::aggregate_statistics::tests::TestAggregate;
//
//     use super::CoaslesceBeforeStreamingAggregate;
//
//     macro_rules! assert_optimized {
//         ($EXPECTED_LINES: expr, $PLAN: expr) => {
//             let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();
//
//             // run optimizer
//             let optimizer = CoaslesceBeforeStreamingAggregate {};
//             let config = ConfigOptions::new();
//             let optimized = optimizer.optimize($PLAN, &config)?;
//             // Now format correctly
//             let plan = displayable(optimized.as_ref()).indent(true).to_string();
//             let actual_lines = trim_plan_display(&plan);
//
//             assert_eq!(
//                 &expected_lines, &actual_lines,
//                 "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
//                 expected_lines, actual_lines
//             );
//         };
//     }
//
//     fn trim_plan_display(plan: &str) -> Vec<&str> {
//         plan.split('\n')
//             .map(|s| s.trim())
//             .filter(|s| !s.is_empty())
//             .collect()
//     }
//
//     #[tokio::test]
//     async fn test_coalesce_does_not_get_added_to_single_partition_inputs() -> Result<()> {
//         // basic test case with the aggregation applied on a source with exact statistics
//         let session_ctx = SessionContext::new();
//         let state = session_ctx.state();
//         let source = mock_data(1).unwrap();
//         let schema = source.schema();
//         let agg = TestAggregate::new_count_column(&schema);
//
//         let partial_agg = FranzStreamingWindowExec::try_new(
//             AggregateMode::Partial,
//             PhysicalGroupBy::default(),
//             vec![agg.count_expr(&schema)],
//             vec![None],
//             source,
//             Arc::clone(&schema),
//             FranzStreamingWindowType::Tumbling(Duration::from_millis(5000)),
//         )
//         .unwrap();
//
//         let plan: Arc<dyn ExecutionPlan> = Arc::new(partial_agg);
//         let optimized = CoaslesceBeforeStreamingAggregate::new()
//             .optimize(Arc::clone(&plan), state.config_options())
//             .unwrap();
//
//         let expected = &[
//             "FranzStreamingWindowExec: mode=Partial, gby=[], aggr=[COUNT(a)], window_type=[Tumbling(5s)]",
//             "MemoryExec: partitions=1, partition_sizes=[1]",
//         ];
//         assert_optimized!(expected, optimized);
//         Ok(())
//     }
//
//     #[tokio::test]
//     async fn test_coalesce_does_gets_added_to_multi_partition_inputs() -> Result<()> {
//         // basic test case with the aggregation applied on a source with exact statistics
//         let session_ctx = SessionContext::new();
//         let state = session_ctx.state();
//         let source = mock_data(2).unwrap();
//         let schema = source.schema();
//         let agg = TestAggregate::new_count_column(&schema);
//
//         let partial_agg = FranzStreamingWindowExec::try_new(
//             AggregateMode::Partial,
//             PhysicalGroupBy::default(),
//             vec![agg.count_expr(&schema)],
//             vec![None],
//             source,
//             Arc::clone(&schema),
//             FranzStreamingWindowType::Tumbling(Duration::from_millis(5000)),
//         )
//         .unwrap();
//
//         let plan: Arc<dyn ExecutionPlan> = Arc::new(partial_agg);
//         let optimized = CoaslesceBeforeStreamingAggregate::new()
//             .optimize(Arc::clone(&plan), state.config_options())
//             .unwrap();
//
//         let expected = &[
//             "FranzStreamingWindowExec: mode=Partial, gby=[], aggr=[COUNT(a)], window_type=[Tumbling(5s)]",
//             "RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=2",
//             "MemoryExec: partitions=2, partition_sizes=[1, 1]",
//         ];
//         assert_optimized!(expected, optimized);
//         Ok(())
//     }
//     fn mock_data(partitioning: usize) -> Result<Arc<MemoryExec>> {
//         let schema = Arc::new(Schema::new(vec![
//             Field::new("a", DataType::Int32, true),
//             Field::new("b", DataType::Int32, true),
//         ]));
//
//         let mut batches = vec![];
//         for _ in 0..partitioning {
//             let batch = RecordBatch::try_new(
//                 Arc::clone(&schema),
//                 vec![
//                     Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
//                     Arc::new(Int32Array::from(vec![Some(4), None, Some(6)])),
//                 ],
//             )?;
//             batches.push(vec![batch]);
//         }
//
//         Ok(Arc::new(MemoryExec::try_new(
//             &batches,
//             Arc::clone(&schema),
//             None,
//         )?))
//     }
// }
