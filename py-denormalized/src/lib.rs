use pyo3::prelude::*;

use datafusion_python::{expr, functions};

pub mod context;
pub mod datastream;

pub mod errors;
pub mod utils;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    // Register the Tokio Runtime as a module attribute so we can reuse it
    m.add(
        "runtime",
        TokioRuntime(tokio::runtime::Runtime::new().unwrap()),
    )?;

    m.add_class::<datastream::PyDataStream>()?;
    m.add_class::<context::PyContext>()?;

    // Register `expr` as a submodule. Matching `datafusion-expr` https://docs.rs/datafusion-expr/latest/datafusion_expr/
    let expr = PyModule::new_bound(py, "expr")?;
    expr::init_module(&expr)?;
    m.add_submodule(&expr)?;

    // Register the functions as a submodule
    let funcs = PyModule::new_bound(py, "functions")?;
    functions::init_module(&funcs)?;
    m.add_submodule(&funcs)?;

    Ok(())
}
