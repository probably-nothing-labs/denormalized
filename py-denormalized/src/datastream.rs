use pyo3::prelude::*;

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::Expr;

use denormalized::datastream::DataStream;

use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_python::expr::{join::PyJoinType, PyExpr};

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

    pub fn select(&self, expr_list: Vec<PyExpr>) -> PyResult<Self> {
        let expr_list: Vec<_> = expr_list.into_iter().map(|e: PyExpr| e.expr).collect();
        // I think I need to fork datafusion-python and change it to depend on our fork of datafusion :(
        let ds = self.ds.select(expr_list).map_err(PyErr::from)?;
        Ok(Self::new(ds))
    }

    pub fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn join_on(
        &self,
        right: PyObject,
        join_type: PyJoinType,
        on_exprs: Vec<PyExpr>,
    ) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn join(
        &self,
        right: PyObject,
        join_type: PyJoinType,
        left_cols: Vec<String>,
        right_cols: Vec<String>,
        filter: Option<PyExpr>,
    ) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn window(
        &self,
        group_expr: Vec<PyExpr>,
        aggr_expr: Vec<PyExpr>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub async fn print_stream(&self) -> PyResult<()> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn print_schema(&self) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn print_plan(&self) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub async fn print_physical_plan(&self) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub async fn sink_kafka(&self, bootstrap_servers: String, topic: String) -> PyResult<()> {
        // Implement the method using the original Rust code
        todo!()
    }
}
