use async_trait::async_trait;
use futures::StreamExt;
use std::fmt::{self, Debug};
use std::{any::Any, sync::Arc};

use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::{
    insert::{DataSink, DataSinkExec},
    DisplayAs, DisplayFormatType, SendableRecordBatchStream,
};
use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::{metrics::MetricsSet, streaming::StreamingTableExec, ExecutionPlan};

use super::{KafkaTopicConfig, KafkaStreamRead};

// Used to createa kafka source
pub struct KafkaTopic(pub Arc<KafkaTopicConfig>);

impl KafkaTopic {
    /// Create a new [`StreamTable`] for the given [`StreamConfig`]
    pub fn new(config: Arc<KafkaTopicConfig>) -> Self {
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
        let mut partition_streams = Vec::with_capacity(self.0.partitions as usize);

        for part in 0..self.0.partitions {
            let read_stream = Arc::new(KafkaStreamRead {
                config: self.0.clone(),
                assigned_partitions: vec![part],
            });
            partition_streams.push(read_stream as _);
        }

        Ok(Arc::new(StreamingTableExec::try_new(
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
impl TableProvider for KafkaTopic {
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection).await;
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if overwrite {
            return not_impl_err!("Overwrite not implemented for KafkaTopic");
        }
        let sink = Arc::new(KafkaSink::new());
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            self.schema(),
            None,
        )))
    }
}

fn create_ordering(schema: &Schema, sort_order: &[Vec<Expr>]) -> Result<Vec<LexOrdering>> {
    let mut all_sort_orders = vec![];

    for exprs in sort_order {
        // Construct PhysicalSortExpr objects from Expr objects:
        let mut sort_exprs = vec![];
        for expr in exprs {
            match expr {
                Expr::Sort(sort) => match sort.expr.as_ref() {
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
                },
                expr => return plan_err!("Expected Expr::Sort in output_ordering, but got {expr}"),
            }
        }
        if !sort_exprs.is_empty() {
            all_sort_orders.push(sort_exprs);
        }
    }
    Ok(all_sort_orders)
}

struct KafkaSink {}

impl Debug for KafkaSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSink")
            // .field("num_partitions", &self.batches.len())
            .finish()
    }
}

impl DisplayAs for KafkaSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_count = "@todo";
                write!(f, "KafkaTable (partitions={partition_count})")
            }
        }
    }
}

impl KafkaSink {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataSink for KafkaSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            if batch.num_rows() > 0 {
                println!(
                    "{}",
                    arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
                );
            }
        }

        Ok(row_count as u64)
    }
}
