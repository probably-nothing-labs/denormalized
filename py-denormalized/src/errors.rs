// Code adapted from python-datafusion https://github.com/apache/datafusion-python

use core::fmt;
use std::error::Error;
use std::fmt::Debug;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use denormalized::common::error::DenormalizedError as InnerDenormalizedError;
use pyo3::{exceptions::PyException, PyErr};

pub type Result<T> = std::result::Result<T, DenormalizedError>;

#[derive(Debug)]
pub enum DenormalizedError {
    ExecutionError(InnerDenormalizedError),
    ArrowError(ArrowError),
    Common(String),
    PythonError(PyErr),
    DataFusionError(DataFusionError),
}

impl fmt::Display for DenormalizedError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DenormalizedError::ExecutionError(e) => write!(f, "Denormalized error: {e:?}"),
            DenormalizedError::ArrowError(e) => write!(f, "Arrow error: {e:?}"),
            DenormalizedError::PythonError(e) => write!(f, "Python error {e:?}"),
            DenormalizedError::Common(e) => write!(f, "{e}"),
            DenormalizedError::DataFusionError(e) => write!(f, "DataFusionError{e}"),
        }
    }
}

impl From<ArrowError> for DenormalizedError {
    fn from(err: ArrowError) -> DenormalizedError {
        DenormalizedError::ArrowError(err)
    }
}

impl From<InnerDenormalizedError> for DenormalizedError {
    fn from(err: InnerDenormalizedError) -> DenormalizedError {
        DenormalizedError::ExecutionError(err)
    }
}

impl From<PyErr> for DenormalizedError {
    fn from(err: PyErr) -> DenormalizedError {
        DenormalizedError::PythonError(err)
    }
}

impl From<DenormalizedError> for PyErr {
    fn from(err: DenormalizedError) -> PyErr {
        match err {
            DenormalizedError::PythonError(py_err) => py_err,
            _ => PyException::new_err(err.to_string()),
        }
    }
}

impl From<DataFusionError> for DenormalizedError {
    fn from(err: DataFusionError) -> DenormalizedError {
        DenormalizedError::DataFusionError(err)
    }
}

impl Error for DenormalizedError {}

pub fn py_type_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{e:?}"))
}

pub fn py_runtime_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
}

pub fn py_unsupported_variant_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{e:?}"))
}

pub fn py_denormalized_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
}
