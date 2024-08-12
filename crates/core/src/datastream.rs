use datafusion::logical_expr::LogicalPlan;
use futures::StreamExt;
use std::{sync::Arc, time::Duration};

use datafusion::common::{DFSchema, DataFusionError, Result};
pub use datafusion::dataframe::DataFrame;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{
    logical_plan::LogicalPlanBuilder, utils::find_window_exprs, Expr, JoinType,
};

use crate::context::Context;
use crate::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use crate::logical_plan::StreamingLogicalPlanBuilder;
use crate::physical_plan::utils::time::TimestampUnit;

#[derive(Clone)]
pub struct DataStream {
    pub df: Arc<DataFrame>,
    pub(crate) context: Arc<Context>,
}

pub trait Joinable {
    fn get_plan(self) -> LogicalPlan;
}
impl Joinable for DataFrame {
    fn get_plan(self) -> LogicalPlan {
        let (_, plan) = self.into_parts();
        plan
    }
}
impl Joinable for DataStream {
    fn get_plan(self) -> LogicalPlan {
        let (_, plan) = self.df.as_ref().clone().into_parts();
        plan
    }
}

impl DataStream {
    /// Return the schema of DataFrame that backs the DataStream
    pub fn schema(&self) -> &DFSchema {
        self.df.schema()
    }

    /// Prints the schema of the underlying dataframe
    /// Useful for debugging chained method calls.
    pub fn print_schema(self) -> Result<Self, DataFusionError> {
        println!("{}", self.df.schema());
        Ok(self)
    }

    /// Prints the underlying logical_plan.
    /// Useful for debugging chained method calls.
    pub fn print_plan(self) -> Result<Self, DataFusionError> {
        println!("{}", self.df.logical_plan().display_indent());
        Ok(self)
    }

    pub fn select(self, expr_list: Vec<Expr>) -> Result<Self, DataFusionError> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();

        let window_func_exprs = find_window_exprs(&expr_list);
        let plan = if window_func_exprs.is_empty() {
            plan
        } else {
            LogicalPlanBuilder::window_plan(plan, window_func_exprs)?
        };
        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, project_plan)),
            context: self.context.clone(),
        })
    }

    pub fn filter(self, predicate: Expr) -> Result<Self> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();

        let plan = LogicalPlanBuilder::from(plan).filter(predicate)?.build()?;

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    // drop_columns, sync, columns: &[&str]
    // count
    pub fn join_on(
        self,
        right: impl Joinable,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<Self, DataFusionError> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();
        let right_plan = right.get_plan();

        let plan = LogicalPlanBuilder::from(plan)
            .join_on(right_plan, join_type, on_exprs)?
            .build()?;

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    pub fn join(
        self,
        right: impl Joinable,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> Result<Self, DataFusionError> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();
        let right_plan = right.get_plan();

        let plan = LogicalPlanBuilder::from(plan)
            .join(
                right_plan,
                join_type,
                (left_cols.to_vec(), right_cols.to_vec()),
                filter,
            )?
            .build()?;

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    pub fn window(
        self,
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

    pub async fn print_stream(self) -> Result<(), DataFusionError> {
        let mut stream: SendableRecordBatchStream =
            self.df.as_ref().clone().execute_stream().await?;
        loop {
            match stream.next().await.transpose() {
                Ok(Some(batch)) => {
                    for i in 0..batch.num_rows() {
                        let row = batch.slice(i, 1);
                        println!(
                            "{}",
                            datafusion::common::arrow::util::pretty::pretty_format_batches(&[row])
                                .unwrap()
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
        self,
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
            .register_table(topic.clone(), Arc::new(sink_topic))
            .await?;

        self.df
            .as_ref()
            .clone()
            .write_table(topic.as_str(), DataFrameWriteOptions::default())
            .await?;

        Ok(())
    }
}
