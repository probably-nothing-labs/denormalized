use pyo3::prelude::*;

use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::common::JoinType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion_python::expr::{join::PyJoinType, PyExpr};

use denormalized::datastream::DataStream;

use crate::errors::{py_denormalized_err, DenormalizedError, Result};
use crate::utils::{get_tokio_runtime, python_print, wait_for_future};

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

impl From<DataStream> for PyDataStream {
    fn from(ds: DataStream) -> Self {
        PyDataStream { ds: Arc::new(ds) }
    }
}

impl From<PyDataStream> for DataStream {
    fn from(py_ds: PyDataStream) -> Self {
        Arc::try_unwrap(py_ds.ds).unwrap_or_else(|arc| (*arc).clone())
    }
}

impl From<Arc<DataStream>> for PyDataStream {
    fn from(ds: Arc<DataStream>) -> Self {
        PyDataStream { ds }
    }
}

impl From<PyDataStream> for Arc<DataStream> {
    fn from(py_ds: PyDataStream) -> Self {
        py_ds.ds
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

    pub fn with_column(&self, name: &str, expr: PyExpr) -> Result<Self> {
        let ds = self.ds.as_ref().clone().with_column(name, expr.into())?;
        Ok(Self::new(ds))
    }

    pub fn drop_columns(&self, columns: Vec<String>) -> Result<Self> {
        let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

        let ds = self.ds.as_ref().clone().drop_columns(&columns_ref)?;
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

    #[pyo3(signature = (right, how, left_cols, right_cols, filter=None))]
    pub fn join(
        &self,
        right: PyDataStream,
        how: &str,
        left_cols: Vec<String>,
        right_cols: Vec<String>,
        filter: Option<PyExpr>,
    ) -> PyResult<Self> {
        let right_ds = right.ds.as_ref().clone();

        let join_type = match how {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "semi" => JoinType::LeftSemi,
            "anti" => JoinType::LeftAnti,
            how => {
                return Err(DenormalizedError::Common(format!(
                    "The join type {how} does not exist or is not implemented"
                ))
                .into());
            }
        };

        let filter = filter.map(|f| f.into());

        let left_cols = left_cols.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let right_cols = right_cols.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();

        let ds =
            self.ds
                .as_ref()
                .clone()
                .join(right_ds, join_type, &left_cols, &right_cols, filter)?;
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
        let window_slide_duration = slide_millis.map(Duration::from_millis);

        let ds = self.ds.as_ref().clone().window(
            groups,
            aggr,
            window_length_duration,
            window_slide_duration,
        )?;
        Ok(Self::new(ds))
    }

    pub fn print_schema(&self, py: Python) -> PyResult<Self> {
        let schema = format!("{}", self.ds.schema());
        python_print(py, schema)?;

        Ok(self.to_owned())
    }

    pub fn print_plan(&self, py: Python) -> PyResult<Self> {
        let plan_str = format!("{}", self.ds.df.logical_plan().display_indent());
        python_print(py, plan_str)?;

        Ok(self.to_owned())
    }

    pub fn print_physical_plan(&self, py: Python) -> PyResult<Self> {
        let ds = self.ds.clone();
        let rt = &get_tokio_runtime(py).0;
        let fut: JoinHandle<denormalized::common::error::Result<String>> = rt.spawn(async move {
            let physical_plan = ds.df.as_ref().clone().create_physical_plan().await?;
            let displayable_plan = DisplayableExecutionPlan::new(physical_plan.as_ref());

            Ok(format!("{}", displayable_plan.indent(true)))
        });

        let str = wait_for_future(py, fut).map_err(py_denormalized_err)??;
        python_print(py, str)?;

        Ok(self.to_owned())
    }

    pub fn print_stream(&self, py: Python) -> PyResult<()> {
        let ds = self.ds.as_ref().clone();
        let rt = &get_tokio_runtime(py).0;
        let fut: JoinHandle<denormalized::common::error::Result<()>> =
            rt.spawn(async move { ds.print_stream().await });

        wait_for_future(py, fut).map_err(py_denormalized_err)??;

        Ok(())
    }

    pub fn sink_kafka(&self, bootstrap_servers: String, topic: String, py: Python) -> PyResult<()> {
        let ds = self.ds.as_ref().clone();
        let rt = &get_tokio_runtime(py).0;

        let fut: JoinHandle<denormalized::common::error::Result<()>> =
            rt.spawn(async move { ds.sink_kafka(bootstrap_servers, topic).await });
        wait_for_future(py, fut).map_err(py_denormalized_err)??;

        Ok(())
    }

    /// Execute the dataframe and pass the resulting recordbatch to a python function
    pub fn sink_python(&self, func: PyObject, py: Python) -> PyResult<()> {
        let ds = self.ds.as_ref().clone();
        let rt = &get_tokio_runtime(py).0;

        let fut: JoinHandle<Result<()>> = rt.spawn(async move {
            let mut stream: SendableRecordBatchStream =
                ds.df.as_ref().clone().execute_stream().await?;

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break, // Explicitly check for ctrl-c and exit
                                                          // loop if it occurs
                    message = stream.next() => {
                        match message.transpose() {
                            Ok(Some(batch)) => {
                                Python::with_gil(|py| -> PyResult<()> {
                                    let batch = batch.clone().to_pyarrow(py)?;
                                    func.call1(py, (batch,))?;
                                    Ok(())
                                })?;
                            },
                            Ok(None) => {},
                            Err(err) => {
                                return Err(err.into());
                            },
                        }
                    }
                }
            }

            Ok(())
        });

        // rt.block_on(fut).map_err(py_denormalized_err)??;
        wait_for_future(py, fut).map_err(py_denormalized_err)??;

        Ok(())
    }
}
