use futures::StreamExt;
use std::{sync::Arc, time::Duration};

pub use datafusion::dataframe::DataFrame;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use datafusion_expr::Expr;

use crate::context::Context;
use crate::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use crate::logical_plan::StreamingLogicalPlanBuilder;
use crate::physical_plan::utils::time::TimestampUnit;

#[derive(Clone)]
pub struct DataStream {
    pub(crate) df: Arc<DataFrame>,
    pub(crate) context: Arc<Context>,
}

impl DataStream {
    pub fn streaming_window(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self, DataFusionError> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();

        let plan = LogicalPlanBuilder::from(plan)
            .streaming_window(group_expr, aggr_expr, window_length, slide)?
            .build()?;
        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    pub async fn print_stream(&self) -> Result<(), DataFusionError> {
        let mut stream: SendableRecordBatchStream =
            self.df.as_ref().clone().execute_stream().await?;
        loop {
            match stream.next().await.transpose() {
                Ok(Some(batch)) => {
                    if batch.num_rows() > 0 {
                        println!(
                            "{}",
                            arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
                        );
                    }
                }
                Ok(None) => {
                    log::warn!("No RecordBatch in stream");
                }
                Err(err) => {
                    log::error!("Error reading stream: {:?}", err);
                    return Err(err);
                }
            }
        }
    }

    pub async fn write_table(
        &self,
        bootstrap_servers: String,
        topic: String,
    ) -> Result<(), DataFusionError> {
        let processed_schema = Arc::new(datafusion::common::arrow::datatypes::Schema::from(
            self.df.schema(),
        ));

        let sink_topic = KafkaTopicBuilder::new(bootstrap_servers.clone())
            .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
            .with_encoding("json")?
            .with_topic(topic.clone())
            .with_schema(processed_schema)
            .build_writer(ConnectionOpts::new())
            .await?;

        self.context
            .register_table(topic.clone(), Arc::new(sink_topic)).await?;

        self.df
            .as_ref()
            .clone()
            .write_table(topic.as_str(), DataFrameWriteOptions::default())
            .await?;

        Ok(())
    }
}
