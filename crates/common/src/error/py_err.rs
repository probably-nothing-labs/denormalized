use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;

use super::DenormalizedError;

impl From<DenormalizedError> for PyErr {
    fn from(error: DenormalizedError) -> Self {
        PyRuntimeError::new_err(format!("{:?}", error))
    }
}
