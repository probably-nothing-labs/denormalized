use datafusion::common::Result;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;

pub(crate) type AccumulatorItem = Box<dyn Accumulator>;

pub(crate) fn create_accumulators(
    aggr_expr: &[AggregateFunctionExpr],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect()
}
