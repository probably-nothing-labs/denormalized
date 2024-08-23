use pyo3::prelude::*;

pub mod context;
pub mod datastream;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    println!("hello world from rust");
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, &m)?)?;

    m.add_class::<datastream::PyDataStream>()?;
    m.add_class::<context::PyContext>()?;

    Ok(())
}
