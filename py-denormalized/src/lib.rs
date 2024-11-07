use pyo3::prelude::*;

pub mod context;
pub mod datastream;

pub mod errors;
pub mod utils;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

/// A Python module implemented in Rust.
#[pymodule(name = "_d_internal")]
fn _py_denormalized_internal(py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<datastream::PyDataStream>()?;
    m.add_class::<context::PyContext>()?;

    datafusion_python::_internal(py, &m)?;
    // Register the functions as a submodule
    // let datafusion = &PyModule::new_bound(py, "datafusion")?;
    // datafusion_python::_internal(py, datafusion)?;
    // m.add_submodule(datafusion)?;

    Ok(())
}
