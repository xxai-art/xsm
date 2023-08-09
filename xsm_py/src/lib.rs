use pyo3::{prelude::*, types::PyBytes};
use pyo3_asyncio::tokio::future_into_py;
use rs_xsm::xxai_msgpacker::Packable;

#[pyclass]
pub struct Client(rs_xsm::Client);

#[pyclass]
pub struct Stream(rs_xsm::Stream);

#[pyfunction]
fn server_host_port(
  py: Python<'_>,
  host: String,
  port: u16,
  username: Option<String>,
  password: Option<String>,
) -> PyResult<&PyAny> {
  pyo3_asyncio::tokio::future_into_py(py, async move {
    let block = 60000;
    let pending = 3 * block;
    let client = Client(
      rs_xsm::Client::conn(
        rs_xsm::Server::host_port(host, port),
        username,
        password,
        Some(block),
        pending as _,
        "C",
      )
      .await?,
    );
    Ok(Python::with_gil(|_| client))
  })
}

#[pymethods]
impl Client {
  pub fn stream(&self, name: String) -> Stream {
    Stream(self.0.stream(name))
  }
  // redis.xadd(
  // stream
  // [
  //   [
  //     pack id
  //     pack args
  //   ]
  // ]
  pub fn xadd(self_: PyRef<'_, Self>, stream: String, id: u64, val: Vec<u8>) -> PyResult<&PyAny> {
    let py = self_.py();
    let c = self_.0.clone();
    future_into_py(py, async move {
      let mut id_bin = Vec::new();
      id.pack(&mut id_bin);
      c.xadd(stream, (&id_bin[..], &val[..])).await?;
      Ok(Python::with_gil(|py| py.None()))
    })
  }
}

#[pymethods]
impl Stream {
  pub fn xackdel(self_: PyRef<'_, Self>, task_id: String) -> PyResult<&PyAny> {
    let py = self_.py();
    let c = self_.0.clone();
    future_into_py(py, async move {
      c.xackdel(task_id).await?;
      Ok(Python::with_gil(|py| py.None()))
    })
  }

  pub fn xpendclaim(self_: PyRef<'_, Self>, limit: u32) -> PyResult<&PyAny> {
    let py = self_.py();
    let c = self_.0.clone();
    future_into_py(py, async move {
      let r = c.xpendclaim(limit).await?;

      Ok(Python::with_gil(|py| {
        r.map_or(vec![], |li| {
          li.into_iter()
            .map(|(retry, t0, t1, id, msg)| {
              let id: PyObject = PyBytes::new(py, &id).into();
              let msg: PyObject = PyBytes::new(py, &msg).into();
              (retry, format!("{t0}-{t1}"), id, msg)
            })
            .collect::<Vec<_>>()
        })
      }))
    })
  }

  pub fn xnext(
    self_: PyRef<'_, Self>,
    limit: u64, // 获取的数量
  ) -> PyResult<&PyAny> {
    let py = self_.py();
    let client = self_.0.clone();
    future_into_py(py, async move {
      let li = client.xnext(limit).await?;

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

/// A Python module implemented in Rust.
#[pymodule]
fn xsmpy(_py: Python, m: &PyModule) -> PyResult<()> {
  // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
  m.add_function(wrap_pyfunction!(server_host_port, m)?)?;
  m.add_class::<Client>()?;
  Ok(())
}
