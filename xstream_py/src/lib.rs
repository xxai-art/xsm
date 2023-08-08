use pyo3::{prelude::*, types::PyBytes};
use pyo3_asyncio::tokio::future_into_py;

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

#[pymethods]
impl Client {
  pub fn xpendclaim(self_: PyRef<'_, Self>, stream: String, limit: u32) -> PyResult<&PyAny> {
    let py = self_.py();
    let client = self_.0.clone();
    future_into_py(py, async move {
      let r: Option<bytes::Bytes> = client.xpendclaim(stream, limit).await?;

      Ok(Python::with_gil(|py| {
        if let Some(r) = r {
          if !r.is_empty() {
            let b0 = r[0];
            if b0 < 6 {
              let end = (b0 + 1) as _;
              let bin_len = &r[1..end];
              let begin = end;
              let end = begin + usize::from_le_bytes(bin_len.try_into().unwrap());

              let bin = &r[begin..end];
              return PyBytes::new(py, &r).into();
            }
          }
        }
        return py.None();
      }))
    })
  }

  pub fn xnext(
    self_: PyRef<'_, Self>,
    key: String,
    count: u64, // 获取的数量
  ) -> PyResult<&PyAny> {
    let py = self_.py();
    let client = self_.0.clone();
    future_into_py(py, async move {
      let li = client.xnext(key, count).await?;

      Ok(Python::with_gil(|py| {
        if let Some(li) = li {
          li.into_iter()
            .map(|(xid, kv)| {
              let mut args = Vec::with_capacity(kv.len());
              for (k, v) in kv {
                let k: PyObject = PyBytes::new(py, &k).into();
                let v: PyObject = PyBytes::new(py, &v).into();
                args.push((k, v));
              }
              (xid, args)
            })
            .collect()
        } else {
          vec![]
        }
      }))
    })
  }
}
// fn sleep_for(py: Python<'_>) -> PyResult<&PyAny> {
//   future_into_py(py, async {
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
