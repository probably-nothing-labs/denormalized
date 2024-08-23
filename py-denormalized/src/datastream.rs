use pyo3::prelude::*;

use datafusion::arrow::datatypes::Schema;
use denormalized::datastream::DataStream;
use std::sync::Arc;

use datafusion::arrow::pyarrow::PyArrowType;

#[pyclass(name = "PyDataStream", module = "denormalized", subclass)]
#[derive(Clone)]
pub struct PyDataStream {
    ds: Arc<DataStream>,
}

impl PyDataStream {
    /// creates a new PyDataFrame
    pub fn new(ds: DataStream) -> Self {
        Self { ds: Arc::new(ds) }
    }
}

#[pymethods]
impl PyDataStream {
    fn __repr__(&self, _py: Python) -> PyResult<String> {
        Ok("PyDataStream".to_string())
    }

    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.ds.schema().into())
    }
}
