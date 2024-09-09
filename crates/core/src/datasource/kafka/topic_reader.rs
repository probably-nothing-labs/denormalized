use async_trait::async_trait;
use std::{any::Any, sync::Arc};

use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion::catalog::Session;
use datafusion::common::{not_impl_err, plan_err, Result};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, SortExpr, TableType};
use datafusion::physical_expr::{expressions, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;

use crate::physical_plan::stream_table::DenormalizedStreamingTableExec;

use super::{KafkaReadConfig, KafkaStreamRead};

// Used to createa kafka source
pub struct TopicReader(pub Arc<KafkaReadConfig>);

impl TopicReader {
    /// Create a new [`StreamTable`] for the given [`StreamConfig`]
    pub fn new(config: Arc<KafkaReadConfig>) -> Self {
        Self(config)
    }

    pub async fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => {
                let projected = self.0.schema.project(p)?;
                create_ordering(&projected, &self.0.order)?
            }
            None => create_ordering(self.0.schema.as_ref(), &self.0.order)?,
        };
        let mut partition_streams = Vec::with_capacity(self.0.partition_count as usize);

        for part in 0..self.0.partition_count {
            let read_stream = Arc::new(KafkaStreamRead {
                config: self.0.clone(),
                assigned_partitions: vec![part],
                exec_node_id: None,
            });
            partition_streams.push(read_stream as _);
        }

        Ok(Arc::new(DenormalizedStreamingTableExec::try_new(
            self.0.schema.clone(),
            partition_streams,
            projection,
            projected_schema,
            true,
            None,
        )?))
    }
}

#[async_trait]
impl TableProvider for TopicReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection).await;
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return not_impl_err!("Writing not implemented for TopicReader please use TopicWriter");
    }
}

fn create_ordering(schema: &Schema, sort_order: &[Vec<SortExpr>]) -> Result<Vec<LexOrdering>> {
    let mut all_sort_orders = vec![];

    for exprs in sort_order {
        // Construct PhysicalSortExpr objects from Expr objects:
        let mut sort_exprs = vec![];
        for sort in exprs {
            match &sort.expr {
                Expr::Column(col) => match expressions::col(&col.name, schema) {
                    Ok(expr) => {
                        sort_exprs.push(PhysicalSortExpr {
                            expr,
                            options: SortOptions {
                                descending: !sort.asc,
                                nulls_first: sort.nulls_first,
                            },
                        });
                    }
                    // Cannot find expression in the projected_schema, stop iterating
                    // since rest of the orderings are violated
                    Err(_) => break,
                },
                expr => {
                    return plan_err!(
                        "Expected single column references in output_ordering, got {expr}"
                    )
                }
            }
        }
        if !sort_exprs.is_empty() {
            all_sort_orders.push(sort_exprs);
        }
    }
    Ok(all_sort_orders)
}
