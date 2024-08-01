use std::sync::Arc;

use datafusion_expr::Accumulator;
use datafusion_physical_expr::AggregateExpr;
pub(crate) type AccumulatorItem = Box<dyn Accumulator>;
use datafusion_common::Result;

pub(crate) fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect()
}
