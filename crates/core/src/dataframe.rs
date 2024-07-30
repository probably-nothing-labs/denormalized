#![allow(missing_docs)]

use async_trait::async_trait;
use std::time::Duration;

pub use datafusion::dataframe::DataFrame;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use crate::logical_plan::StreamingLogicalPlanBuilder;
use datafusion_expr::Expr;

use futures::StreamExt;

#[async_trait]
pub trait StreamingDataframe {
    type Error;

    fn streaming_window(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized;

    async fn print_stream(self) -> Result<(), Self::Error>
    where
        Self: Sized;
}

#[async_trait]
impl StreamingDataframe for DataFrame {
    type Error = DataFusionError;

    fn streaming_window(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self, Self::Error> {
        let (session_state, plan) = self.into_parts();

        let plan = LogicalPlanBuilder::from(plan)
            .streaming_window(group_expr, aggr_expr, window_length, slide)?
            .build()?;
        Ok(DataFrame::new(session_state, plan))
    }

    async fn print_stream(self) -> Result<(), Self::Error> {
        let mut stream: SendableRecordBatchStream = self.execute_stream().await?;
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
}
