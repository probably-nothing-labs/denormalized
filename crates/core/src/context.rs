use std::sync::{Arc, RwLock};

use datafusion::datasource::TableProvider;
use datafusion::execution::{
    config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv,
    session_state::SessionState,
};
use datafusion_common::{DataFusionError, Result};

use crate::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder, TopicReader, TopicWriter};
use crate::datastream::DataStream;
use crate::physical_optimizer::CoaslesceBeforeStreamingAggregate;
use crate::query_planner::StreamingQueryPlanner;

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
        let state = SessionState::new_with_config_rt(config, runtime)
            .with_query_planner(Arc::new(StreamingQueryPlanner {}))
            // @todo -- we'll need to remove the projection optimizer rule
            .add_physical_optimizer_rule(Arc::new(CoaslesceBeforeStreamingAggregate::new()));

        Ok(Self {
            session_conext: Arc::new(RwLock::new(SessionContext::new_with_state(state))),
        })
    }

    pub async fn from_topic(&self, topic: TopicReader) -> Result<DataStream, DataFusionError> {
        let topic_name = topic.0.topic.clone();

        self.register_table(topic_name.clone(), Arc::new(topic))?;

        let df = self
            .session_conext
            .read()
            .expect("Unlock datafusion context")
            .table(topic_name.as_str())
            .await?;

        let ds = DataStream {
            df: Arc::new(df),
            context: Arc::new(self.clone()),
        };
        Ok(ds)
    }

    pub fn register_table(
        &self,
        name: String,
        table: Arc<impl TableProvider + 'static>,
    ) -> Result<(), DataFusionError> {
        self.session_conext
            .write()
            .expect("Unlock datafusion context")
            .register_table(name.as_str(), table.clone())?;

        Ok(())
    }
}
