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
        let ds = self
            .ds
            .as_ref()
            .clone()
            .select(expr_list)
            .map_err(PyErr::from)?;
        Ok(Self::new(ds))
    }

    pub fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        let ds = self.ds.as_ref().clone().filter(predicate.into())?;
        Ok(Self::new(ds))
    }

    pub fn join_on(
        &self,
        _right: PyDataStream,
        _join_type: PyJoinType,
        _on_exprs: Vec<PyExpr>,
    ) -> PyResult<Self> {
        todo!()
    }

    #[pyo3(signature = (right, join_type, left_cols, right_cols, filter=None))]
    pub fn join(
        &self,
        right: PyDataStream,
        join_type: PyJoinType,
        left_cols: Vec<String>,
        right_cols: Vec<String>,
        filter: Option<PyExpr>,
    ) -> PyResult<Self> {
        let right_ds = right.ds.as_ref().clone();

        let filter = filter.map(|f| f.into());

        let ds = self.ds.as_ref().clone().join(
            right_ds,
            join_type.into(),
            &left_cols,
            &right_cols,
            filter,
        )?;
        Ok(Self::new(ds))
    }

    #[pyo3(signature = (group_expr, aggr_expr, window_length_millis, slide_millis=None))]
    pub fn window(
        &self,
        group_expr: Vec<PyExpr>,
        aggr_expr: Vec<PyExpr>,
        window_length_millis: u64,
        slide_millis: Option<u64>,
    ) -> PyResult<Self> {
        let groups = group_expr
            .into_iter()
            .map(|py_expr| py_expr.into())
            .collect();
        let aggr = aggr_expr
            .into_iter()
            .map(|py_expr| py_expr.into())
            .collect();

        // Use u64 for durations since using PyDelta type requires non-Py_LIMITED_API to be
        // enabled
        let window_length_duration = Duration::from_millis(window_length_millis);
        let window_slide_duration = slide_millis.map(|d| Duration::from_millis(d));

        let ds = self.ds.as_ref().clone().window(
            groups,
            aggr,
            window_length_duration,
            window_slide_duration,
        )?;
        Ok(Self::new(ds))
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
