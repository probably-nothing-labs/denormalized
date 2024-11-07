use datafusion::common::runtime::SpawnedTask;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use futures::StreamExt;
use log::debug;
use log::info;
use std::{sync::Arc, time::Duration};
use tokio::signal;
use tokio::sync::watch;

use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{
    logical_plan::LogicalPlanBuilder, utils::find_window_exprs, Expr, JoinType,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;

use crate::config_extensions::denormalized_config::DenormalizedConfig;
use crate::context::Context;
use crate::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use crate::logical_plan::StreamingLogicalPlanBuilder;
use crate::physical_plan::utils::time::TimestampUnit;
use crate::state_backend::slatedb::get_global_slatedb;
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
    shutdown_tx: watch::Sender<bool>,   // Sender to trigger shutdown
    shutdown_rx: watch::Receiver<bool>, // Receiver to listen for shutdown signal
}

impl DataStream {
    pub fn new(df: Arc<DataFrame>, context: Arc<Context>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        DataStream {
            df,
            context,
            shutdown_tx,
            shutdown_rx,
        }
    }

    fn start_shutdown_listener(&self) {
        let shutdown_tx = self.shutdown_tx.clone();

        tokio::spawn(async move {
            let mut terminate_signal = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to listen for SIGTERM");

            loop {
                tokio::select! {
                    _ = signal::ctrl_c() => {
                        println!("Received Ctrl+C, initiating shutdown...");
                    },
                    _ = terminate_signal.recv() => {
                        println!("Received SIGTERM, initiating shutdown...");
                    },
                }
                shutdown_tx.send(true).unwrap();
            }
        });
    }

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
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
        })
    }

    // Apply a filter
    pub fn filter(self, predicate: Expr) -> Result<Self> {
        let (session_state, plan) = self.df.as_ref().clone().into_parts();

        let plan = LogicalPlanBuilder::from(plan).filter(predicate)?.build()?;

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
        })
    }

    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self> {
        Ok(Self {
            df: Arc::new(self.df.as_ref().clone().with_column(name, expr)?),
            context: self.context.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
        })
    }

    pub fn drop_columns(self, columns: &[&str]) -> Result<Self> {
        Ok(Self {
            df: Arc::new(self.df.as_ref().clone().drop_columns(columns)?),
            context: self.context.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
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
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
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
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
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
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
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
        let node_id = physical_plan.node_id();
        debug!("topline node id = {:?}", node_id);
        let displayable_plan = DisplayableExecutionPlan::new(physical_plan.as_ref());

        println!("{}", displayable_plan.indent(true));

        Ok(Self {
            df: Arc::new(DataFrame::new(session_state, plan)),
            context: self.context.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
        })
    }

    /// execute the stream and print the results to stdout.
    /// Mainly used for development and debugging
    pub async fn print_stream(mut self) -> Result<()> {
        self.start_shutdown_listener();

        let mut maybe_orchestrator_handle = None;

        let config = self.context.session_context.copied_config();
        let config_options = config.options().extensions.get::<DenormalizedConfig>();

        let should_checkpoint = config_options.map_or(false, |c| c.checkpoint);

        if should_checkpoint {
            let mut orchestrator = Orchestrator::default();
            let cloned_shutdown_rx = self.shutdown_rx.clone();
            let orchestrator_handle =
                SpawnedTask::spawn_blocking(move || orchestrator.run(10, cloned_shutdown_rx));

            maybe_orchestrator_handle = Some(orchestrator_handle)
        }

        let mut stream: SendableRecordBatchStream =
            self.df.as_ref().clone().execute_stream().await?;

        // Stream loop with shutdown check
        loop {
            tokio::select! {
                // Check if shutdown signal has changed
                _ = self.shutdown_rx.changed() => {
                    info!("Graceful shutdown initiated, exiting stream loop...");

                    break;
                }
                // Handle the next batch from the DataFusion stream
                next_batch = stream.next() => {
                    match next_batch.transpose() {
                        Ok(Some(batch)) => {
                            println!(
                                "{}",
                                datafusion::common::arrow::util::pretty::pretty_format_batches(&[batch])
                                    .unwrap()
                            );
                        }
                        Ok(None) => {
                            info!("No more RecordBatch in stream");
                            break;  // End of stream
                        }
                        Err(err) => {
                            log::error!("Error reading stream: {:?}", err);
                            return Err(err.into());
                        }
                    }
                }
            }
        }

        log::info!("Stream processing stopped. Cleaning up...");

        if should_checkpoint {
            let state_backend = get_global_slatedb();
            if let Ok(db) = state_backend {
                log::info!("Closing the state backend (slatedb)...");
                db.close().await.unwrap();
            }
        }

        // Join the orchestrator handle if it exists, ensuring it is joined and awaited
        if let Some(orchestrator_handle) = maybe_orchestrator_handle {
            log::info!("Waiting for orchestrator task to complete...");
            match orchestrator_handle.join_unwind().await {
                Ok(_) => log::info!("Orchestrator task completed successfully."),
                Err(e) => log::error!("Error joining orchestrator task: {:?}", e),
            }
        }
        Ok(())
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
