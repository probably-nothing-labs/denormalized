use pyo3::prelude::*;

use std::sync::Arc;

use denormalized::context::Context;
use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::datastream::DataStream;
use denormalized::physical_plan::utils::time::TimestampUnit;

use tokio::task::JoinHandle;

use crate::datastream::PyDataStream;
use crate::errors::py_denormalized_err;
use crate::utils::{get_tokio_runtime, wait_for_future};

#[pyclass(module = "denormalized", subclass)]
#[derive(Clone)]
pub struct PyContext {
    context: Arc<Context>,
}

impl PyContext {}

impl From<Context> for PyContext {
    fn from(context: Context) -> Self {
        PyContext {
            context: Arc::new(context),
        }
    }
}

impl From<PyContext> for Context {
    fn from(py_context: PyContext) -> Self {
        Arc::try_unwrap(py_context.context).unwrap_or_else(|arc| (*arc).clone())
    }
}

impl From<Arc<Context>> for PyContext {
    fn from(context: Arc<Context>) -> Self {
        PyContext { context }
    }
}

impl From<PyContext> for Arc<Context> {
    fn from(py_context: PyContext) -> Self {
        py_context.context
    }
}

#[pymethods]
impl PyContext {
    /// creates a new PyDataFrame
    #[new]
    pub fn new(py: Python) -> PyResult<Self> {
        let rt = &get_tokio_runtime(py).0;
        let fut: JoinHandle<denormalized::common::error::Result<Context>> =
            rt.spawn(async move { Ok(Context::new()?) });

        let context = wait_for_future(py, fut).map_err(py_denormalized_err)??;

        Ok(Self {
            context: Arc::new(context),
        })
    }

    fn __repr__(&self, _py: Python) -> PyResult<String> {
        Ok("PyContext".to_string())
    }

    fn __str__(&self, _py: Python) -> PyResult<String> {
        Ok("PyContext".to_string())
    }

    pub fn from_topic(
        &self,
        py: Python,
        topic: String,
        sample_json: String,
        bootstrap_servers: String,
        timestamp_column: String,
        group_id: String,
    ) -> PyResult<PyDataStream> {
        let context = self.context.clone();
        let rt = &get_tokio_runtime(py).0;
        let fut: JoinHandle<denormalized::common::error::Result<DataStream>> =
            rt.spawn(async move {
                let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

                let source_topic = topic_builder
                    .with_timestamp(timestamp_column, TimestampUnit::Int64Millis)
                    .with_encoding("json")?
                    .with_topic(topic)
                    .infer_schema_from_json(sample_json.as_str())?
                    .build_reader(ConnectionOpts::from([
                        ("auto.offset.reset".to_string(), "latest".to_string()),
                        ("group.id".to_string(), group_id.to_string()),
                    ]))
                    .await?;

                context.from_topic(source_topic).await
            });

        let ds = wait_for_future(py, fut).map_err(py_denormalized_err)??;

        Ok(PyDataStream::new(ds))
    }
}
