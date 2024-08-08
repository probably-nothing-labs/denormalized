use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::AggregateExpr;
use datafusion::common::Result;

pub(crate) type AccumulatorItem = Box<dyn Accumulator>;

pub(crate) fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect()
}
