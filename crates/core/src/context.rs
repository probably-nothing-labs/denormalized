use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::{
    config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv,
    session_state::SessionStateBuilder,
};

use crate::datasource::kafka::TopicReader;
use crate::datastream::DataStream;
use crate::physical_optimizer::CoaslesceBeforeStreamingAggregate;
use crate::query_planner::StreamingQueryPlanner;
use crate::utils::get_default_optimizer_rules;

use denormalized_common::error::{DenormalizedError, Result};

#[derive(Clone)]
pub struct Context {
    pub session_conext: Arc<SessionContext>,
}

impl Context {
    pub fn new() -> Result<Self, DenormalizedError> {
        let config = SessionConfig::new()
            .set(
                "datafusion.execution.batch_size",
                &datafusion::common::ScalarValue::UInt64(Some(32)),
            )
            // coalesce_batches slows down the pipeline and increases latency as it tries to concat
            // small batches together so we disable it.
            .set(
                "datafusion.execution.coalesce_batches",
                &datafusion::common::ScalarValue::Boolean(Some(false)),
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
            session_conext: Arc::new(SessionContext::new_with_state(state)),
        })
    }

    pub async fn from_topic(&self, topic: TopicReader) -> Result<DataStream, DenormalizedError> {
        let topic_name = topic.0.topic.clone();

        self.register_table(topic_name.clone(), Arc::new(topic))
            .await?;

        let df = self.session_conext.table(topic_name.as_str()).await?;

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
    ) -> Result<(), DenormalizedError> {
        self.session_conext
            .register_table(name.as_str(), table.clone())?;

        Ok(())
    }
}
