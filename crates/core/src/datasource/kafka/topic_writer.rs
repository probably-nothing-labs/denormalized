use async_trait::async_trait;
use futures::StreamExt;
use std::fmt::{self, Debug};
use std::time::Duration;
use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;

use datafusion::catalog::Session;
use datafusion::common::{not_impl_err, Result};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::{
    insert::{DataSink, DataSinkExec},
    metrics::MetricsSet,
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};

use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

use super::KafkaWriteConfig;
use crate::utils::row_encoder::{JsonRowEncoder, RowEncoder};

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
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return not_impl_err!("Reading not implemented for TopicWriter please use TopicReader");
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if overwrite {
            return not_impl_err!("Overwrite not implemented for TopicWriter");
        }
        let sink = Arc::new(KafkaSink::new(self.0.clone()));
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            self.schema(),
            None,
        )))
    }
}

struct KafkaSink {
    producer: FutureProducer,
    config: Arc<KafkaWriteConfig>,
}

impl KafkaSink {
    fn new(config: Arc<KafkaWriteConfig>) -> Self {
        let producer = config.make_producer().unwrap();

        Self { producer, config }
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
        let topic = self.config.topic.as_str();

        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();

            let encoder = JsonRowEncoder {};
            let rows = encoder.encode(&batch)?;

            for row in rows {
                let record = FutureRecord::<[u8], _>::to(topic).payload(&row);
                // .key(key.as_str()),

                if let Err(msg) = self.producer.send(record, Duration::from_secs(0)).await {
                    tracing::error!("{}", msg.0);
                }
            }
        }

        Ok(row_count as u64)
    }
}

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
                let partition_count = self.config.partition_count;
                write!(f, "KafkaTable (partitions={partition_count})")
            }
        }
    }
}
