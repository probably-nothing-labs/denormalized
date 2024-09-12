use pyo3::prelude::*;

pub mod context;
pub mod datastream;

pub mod errors;
pub mod utils;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<datastream::PyDataStream>()?;
    m.add_class::<context::PyContext>()?;

    Ok(())
}
