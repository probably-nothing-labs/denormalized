#![allow(missing_docs)]

use async_trait::async_trait;
use std::time::Duration;

pub use datafusion::dataframe::DataFrame;
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use datafusion_expr::Expr;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::SendableRecordBatchStream;

use futures::StreamExt;

use df_streams_sinks::FranzSink;


#[async_trait]
pub trait Sinkable {
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

    async fn sink(self, sink: Box<dyn FranzSink>) -> Result<(), Self::Error>
    where
        Self: Sized;

    fn foo(self) -> ();
}

#[async_trait]
impl Sinkable for DataFrame {
    type Error = DataFusionError;

    fn foo(self) -> () {

    }

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

    async fn sink(self, mut sink: Box<dyn FranzSink>) -> Result<(), Self::Error> {
        let mut stream: SendableRecordBatchStream = self.execute_stream().await?;
        loop {
            match stream.next().await.transpose() {
                Ok(Some(batch)) => {
                    if batch.num_rows() > 0 {
                        sink.write_records(batch).await?;
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
