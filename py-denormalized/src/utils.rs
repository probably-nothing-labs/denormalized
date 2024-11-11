use crate::TokioRuntime;
// use datafusion::logical_expr::Volatility;
use pyo3::prelude::*;
use std::future::Future;
use tokio::runtime::Runtime;

/// Utility to get the Tokio Runtime from Python
pub(crate) fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let datafusion = py.import_bound("denormalized._d_internal").unwrap();
    let tmp = datafusion.getattr("runtime").unwrap();
    match tmp.extract::<PyRef<TokioRuntime>>() {
        Ok(runtime) => runtime,
        Err(_e) => {
            let rt = TokioRuntime(tokio::runtime::Runtime::new().unwrap());
            let obj: Bound<'_, TokioRuntime> = Py::new(py, rt).unwrap().into_bound(py);
            obj.extract().unwrap()
        }
    }
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    // allow_threads explicitly releases the GIL until the future returns
    py.allow_threads(|| runtime.block_on(f))
}

/// Print a string to the python console
pub fn python_print(py: Python, str: String) -> PyResult<()> {
    // Import the Python 'builtins' module to access the print function
    // Note that println! does not print to the Python debug console and is not visible in notebooks for instance
    let print = py.import_bound("builtins")?.getattr("print")?;
    print.call1((str,))?;
    Ok(())
}

// pub(crate) fn parse_volatility(value: &str) -> Result<Volatility, DataFusionError> {
//     Ok(match value {
//         "immutable" => Volatility::Immutable,
//         "stable" => Volatility::Stable,
//         "volatile" => Volatility::Volatile,
//         value => {
//             return Err(DataFusionError::Common(format!(
//                 "Unsupportad volatility type: `{value}`, supported \
//                  values are: immutable, stable and volatile."
//             )))
//         }
//     })
// }
