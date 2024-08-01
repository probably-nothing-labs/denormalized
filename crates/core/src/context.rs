use std::sync::Arc;
use tokio::sync::RwLock;

use datafusion::datasource::TableProvider;
use datafusion::execution::{
    config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv,
    session_state::SessionStateBuilder,
};
use datafusion_common::{DataFusionError, Result};

use crate::datasource::kafka::TopicReader;
use crate::datastream::DataStream;
use crate::physical_optimizer::CoaslesceBeforeStreamingAggregate;
use crate::query_planner::StreamingQueryPlanner;
use crate::utils::get_default_optimizer_rules;

#[derive(Clone)]
pub struct Context {
    session_conext: Arc<RwLock<SessionContext>>,
}

impl Context {
    pub fn new() -> Result<Self, DataFusionError> {
        let config = SessionConfig::new().set(
            "datafusion.execution.batch_size",
            datafusion_common::ScalarValue::UInt64(Some(32)),
        );
        let runtime = Arc::new(RuntimeEnv::default());

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_query_planner(Arc::new(StreamingQueryPlanner {}))
            .with_optimizer_rules(get_default_optimizer_rules())
            .with_physical_optimizer_rule(Arc::new(CoaslesceBeforeStreamingAggregate::new()))
            .build();

        Ok(Self {
            session_conext: Arc::new(RwLock::new(SessionContext::new_with_state(state))),
        })
    }

    pub async fn from_topic(&self, topic: TopicReader) -> Result<DataStream, DataFusionError> {
        let topic_name = topic.0.topic.clone();

        self.register_table(topic_name.clone(), Arc::new(topic)).await?;

        let df = self
            .session_conext
            .read()
            .await
            .table(topic_name.as_str())
            .await?;

        let ds = DataStream {
            df: Arc::new(df),
            context: Arc::new(self.clone()),
        };
        Ok(ds)
    }

    pub async fn register_table(
        &self,
        name: String,
        table: Arc<impl TableProvider + 'static>,
    ) -> Result<(), DataFusionError> {
        self.session_conext
            .write()
            .await
            .register_table(name.as_str(), table.clone())?;

        Ok(())
    }
}
