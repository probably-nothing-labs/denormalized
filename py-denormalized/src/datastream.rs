use pyo3::prelude::*;

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_python::expr::{join::PyJoinType, PyExpr};

use denormalized::datastream::DataStream;

use crate::errors::py_denormalized_err;
use crate::utils::{get_tokio_runtime, wait_for_future};

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
        Ok("__repr__ PyDataStream".to_string())
    }

    fn __str__(&self, _py: Python) -> PyResult<String> {
        Ok("__str__ PyDataStream".to_string())
    }

    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.ds.schema().into())
    }

    pub fn select(&self, expr_list: Vec<PyExpr>) -> PyResult<Self> {
        let expr_list: Vec<_> = expr_list.into_iter().map(|e: PyExpr| e.expr).collect();

        let ds = self.ds.as_ref().clone().select(expr_list)?;
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

        let left_cols = left_cols.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let right_cols = right_cols.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();

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

    pub fn print_expr(&self, expr: PyExpr) -> () {
        println!("{:?}", expr);
    }

    pub fn print_stream(&self, py: Python) -> PyResult<()> {
        // Implement the method using the original Rust code
        let ds = self.ds.clone();
        let rt = &get_tokio_runtime(py).0;
        let fut: JoinHandle<denormalized::common::error::Result<()>> =
            rt.spawn(async move { ds.print_stream().await });

        let _ = wait_for_future(py, fut).map_err(py_denormalized_err)??;

        Ok(())
    }

    pub fn print_schema(&self) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn print_plan(&self) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn print_physical_plan(&self) -> PyResult<Self> {
        // Implement the method using the original Rust code
        todo!()
    }

    pub fn sink_kafka(&self, _bootstrap_servers: String, _topic: String) -> PyResult<()> {
        // Implement the method using the original Rust code
        todo!()
    }
}
