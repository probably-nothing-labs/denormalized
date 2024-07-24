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
use datafusion_physical_plan::{metrics::MetricsSet, ExecutionPlan};

use super::KafkaWriteConfig;

// Used to createa kafka source
pub struct TopicWriter(pub Arc<KafkaWriteConfig>);

impl TopicWriter {
    /// Create a new [`StreamTable`] for the given [`StreamConfig`]
    pub fn new(config: Arc<KafkaWriteConfig>) -> Self {
        Self(config)
    }
}

#[async_trait]
impl TableProvider for TopicWriter {
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return not_impl_err!("Reading not implemented for TopicWriter please use TopicReader");
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if overwrite {
            return not_impl_err!("Overwrite not implemented for TopicWriter");
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
