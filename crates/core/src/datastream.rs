use datafusion::common::runtime::SpawnedTask;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use denormalized_orchestrator::orchestrator;
use futures::StreamExt;
use std::{sync::Arc, time::Duration};

use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{
    logical_plan::LogicalPlanBuilder, utils::find_window_exprs, Expr, JoinType,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;

use crate::context::Context;
use crate::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use crate::logical_plan::StreamingLogicalPlanBuilder;
use crate::physical_plan::utils::time::TimestampUnit;
use denormalized_orchestrator::orchestrator::Orchestrator;

use denormalized_common::error::Result;

/// The primary interface for building a streaming job
///
/// Wraps the DataFusion DataFrame and context objects and provides methods
/// for constructing and executing streaming pipelines.
#[derive(Clone)]
pub struct DataStream {
    pub df: Arc<DataFrame>,
    pub(crate) context: Arc<Context>,
}

impl DataStream {
    // Select columns in the output stream
    pub fn select(self, expr_list: Vec<Expr>) -> Result<Self> {
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

    // Apply a filter
    pub fn filter(self, predicate: Expr) -> Result<Self> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();

        let plan = LogicalPlanBuilder::from(plan).filter(predicate)?.build()?;

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self> {
        Ok(Self {
            df: Arc::new(self.df.as_ref().clone().with_column(name, expr)?),
            context: self.context.clone(),
        })
    }

    // Join two streams using the specified expression
    pub fn join_on(
        self,
        right: impl Joinable,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<Self> {
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

    // Join two streams together using explicitly specified columns
    // Also supports joining a DataStream with a DataFrame object
    pub fn join(
        self,
        right: impl Joinable,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> Result<Self> {
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

    /// create a streaming window
    pub fn window(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();

        let plan = LogicalPlanBuilder::from(plan)
            .streaming_window(group_expr, aggr_expr, window_length, slide)?
            .build()?;
        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    /// Return the schema of DataFrame that backs the DataStream
    pub fn schema(&self) -> &DFSchema {
        self.df.schema()
    }

    /// Prints the schema of the underlying dataframe
    /// Useful for debugging chained method calls.
    pub fn print_schema(&self) -> &Self {
        println!("{}", self.df.schema());
        self
    }

    /// Prints the underlying logical plan.
    /// Useful for debugging chained method calls.
    pub fn print_plan(&self) -> &Self {
        println!("{}", self.df.logical_plan().display_indent());
        self
    }

    /// Prints the underlying physical plan.
    /// Useful for debugging and development
    pub async fn print_physical_plan(self) -> Result<Self> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();
        let physical_plan = self.df.as_ref().clone().create_physical_plan().await?;
        let displayable_plan = DisplayableExecutionPlan::new(physical_plan.as_ref());

        println!("{}", displayable_plan.indent(true));

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
        })
    }

    /// execute the stream and print the results to stdout.
    /// Mainly used for development and debugging
    pub async fn print_stream(self) -> Result<()> {
        if orchestrator::SHOULD_CHECKPOINT {
            let plan = self.df.as_ref().clone().create_physical_plan().await?;
            let node_ids = extract_node_ids_and_partitions(&plan);
            let max_buffer_size = node_ids.iter().map(|x| x.1).sum::<usize>();
            let mut orchestrator = Orchestrator::default();
            SpawnedTask::spawn_blocking(move || orchestrator.run(max_buffer_size));
        }

        let mut stream: SendableRecordBatchStream =
            self.df.as_ref().clone().execute_stream().await?;
        loop {
            match stream.next().await.transpose() {
                Ok(Some(batch)) => {
                    println!(
                        "{}",
                        datafusion::common::arrow::util::pretty::pretty_format_batches(&[batch])
                            .unwrap()
                    );
                }
                Ok(None) => {
                    log::warn!("No RecordBatch in stream");
                }
                Err(err) => {
                    log::error!("Error reading stream: {:?}", err);
                    return Err(err.into());
                }
            }
        }
    }

    /// execute the stream and write the results to a give kafka topic
    pub async fn sink_kafka(self, bootstrap_servers: String, topic: String) -> Result<()> {
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

/// Trait that allows both DataStream and DataFrame objects to be joined to
/// the current DataStream
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

fn extract_node_ids_and_partitions(plan: &Arc<dyn ExecutionPlan>) -> Vec<(Option<usize>, usize)> {
    let node_id = plan.node_id();
    let partitions = plan.output_partitioning().partition_count();
    let mut traversals: Vec<(Option<usize>, usize)> = vec![];

    for child in plan.children() {
        let mut traversal = extract_node_ids_and_partitions(child);
        traversals.append(&mut traversal);
    }
    traversals.push((node_id, partitions));
    traversals
}
