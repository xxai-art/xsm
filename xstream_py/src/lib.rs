use pyo3::prelude::*;

#[pyclass]
pub struct Client(xstream::Client);

#[pyfunction]
fn server_host_port(
  py: Python<'_>,
  host: String,
  port: u16,
  username: Option<String>,
  password: Option<String>,
) -> PyResult<&PyAny> {
  pyo3_asyncio::tokio::future_into_py(py, async move {
    let client = Client(
      xstream::Client::conn(xstream::Server::host_port(host, port), username, password).await?,
    );
    Ok(Python::with_gil(|_| client))
  })
}

// #[pymethods]
// impl Client {
//   pub fn xnext(
//     &self,
//     py: Python<'_>,
//     key: String,
//     count: u64, // 获取的数量
//   ) -> PyResult<&PyAny> {
//     pyo3_asyncio::tokio::future_into_py(py, async move {
//       let r = self.0.xnext(key, count).await;
//       dbg!(r);
//
//       Ok(Python::with_gil(|_| 1234))
//     })
//   }
// }
// fn sleep_for(py: Python<'_>) -> PyResult<&PyAny> {
//   pyo3_asyncio::tokio::future_into_py(py, async {
//     tokio::time::sleep(std::time::Duration::from_secs(3)).await;
//     Ok(Python::with_gil(|py| py.None()))
//   })
// }
//
// /// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//   Ok((a + b).to_string())
// }

/// A Python module implemented in Rust.
#[pymodule]
fn xstream_py(_py: Python, m: &PyModule) -> PyResult<()> {
  // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
  m.add_function(wrap_pyfunction!(server_host_port, m)?)?;
  m.add_class::<Client>()?;
  Ok(())
}
