use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use std::sync::Arc;

use denormalized::context::Context;
use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::physical_plan::utils::time::TimestampUnit;

use crate::datastream::PyDataStream;

#[pyclass(module = "denormalized", subclass)]
#[derive(Clone)]
pub struct PyContext {
    context: Arc<Context>,
}

impl PyContext {}

#[pymethods]
impl PyContext {
    /// creates a new PyDataFrame
    #[new]
    pub fn new() -> PyResult<Self> {
        if let Ok(ds) = Context::new() {
            Ok(Self {
                context: Arc::new(ds),
            })
        } else {
            Err(PyValueError::new_err("Failed to create new PyContext"))
        }
    }

    fn __repr__(&self, _py: Python) -> PyResult<String> {
        Ok("PyContext".to_string())
    }

    #[pyo3(signature = (topic, sample_json, bootstrap_servers))]
    pub async fn from_topic(
        &self,
        topic: String,
        sample_json: String,
        bootstrap_servers: String,
    ) -> PyResult<PyDataStream> {
        let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

        let source_topic = topic_builder
            .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
            .with_encoding("json")?
            .with_topic(topic)
            .infer_schema_from_json(sample_json.as_str())?
            .build_reader(ConnectionOpts::from([
                ("auto.offset.reset".to_string(), "latest".to_string()),
                ("group.id".to_string(), "sample_pipeline".to_string()),
            ]))
            .await?;

        let ds = self.context.from_topic(source_topic).await.unwrap();

        Ok(PyDataStream::new(ds))
    }
}
