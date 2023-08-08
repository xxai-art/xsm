#![feature(lazy_cell)]

use anyhow::Result;
use bytes::Bytes;
pub use fred::{
  interfaces::{ClientLike, StreamsInterface},
  prelude::{
    PerformanceConfig, ReconnectPolicy, RedisClient, RedisConfig, RedisErrorKind::Unknown,
    ServerConfig,
  },
  types::XID,
};
use gethostname::gethostname;
use lazy_static::lazy_static;

lazy_static! {
  pub static ref HOSTNAME: String = gethostname().into_string().unwrap();
}

#[derive(Clone)]
pub struct Client {
  c: RedisClient,
}

const BLOCK: Option<u64> = Some(60000);
const GROUP: &str = "C";
pub struct Server {
  c: ServerConfig,
}

impl Server {
  pub fn cluster(host_port_li: Vec<(String, u16)>) -> Self {
    Self {
      c: ServerConfig::Clustered {
        hosts: host_port_li
          .into_iter()
          .map(|(host, port)| fred::types::Server {
            host: host.into(),
            port,
            tls_server_name: None,
          })
          .collect(),
      },
    }
  }

  pub fn host_port(host: String, port: u16) -> Self {
    Self {
      c: ServerConfig::Centralized {
        server: fred::types::Server {
          host: host.into(),
          port,
          tls_server_name: None,
        },
      },
    }
  }
}

impl From<Server> for ServerConfig {
  fn from(s: Server) -> Self {
    s.c
  }
}

impl Client {
  pub async fn conn(
    server: impl Into<ServerConfig>,
    username: Option<String>,
    password: Option<String>,
  ) -> Result<Self> {
    let version = fred::types::RespVersion::RESP3;
    let mut conf = RedisConfig {
      version,
      ..Default::default()
    };
    conf.server = server.into();
    conf.username = username;
    conf.password = password;
    let perf = PerformanceConfig::default();
    let policy = ReconnectPolicy::default();
    let client = RedisClient::new(conf, Some(perf), Some(policy));

    // connect to the server, returning a handle to the task that drives the connection
    let _ = client.connect();
    client.wait_for_connect().await?;
    Ok(Self { c: client })
  }

  pub async fn xnext(
    &self,
    key: impl AsRef<str>,
    count: u64, // 获取的数量
  ) -> Result<Option<Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>>> {
    let key = key.as_ref();
    let count = Some(count);
    let hostname = &*HOSTNAME;

    match self
      .c
      .xreadgroup::<Vec<(Bytes, _)>, _, _, _, _>(
        GROUP,
        hostname,
        count,
        BLOCK,
        false,
        key,
        XID::NewInGroup,
      )
      .await
    {
      Ok(mut r) => Ok(if let Some(r) = r.pop() { r.1 } else { None }),
      Err(err) => {
        if err.kind() == &Unknown && err.details().starts_with("NOGROUP ") {
          self
            .c
            .xgroup_create(key, GROUP, XID::Manual("0".into()), true)
            .await?;
          return Ok(
            self
              .c
              .xreadgroup(GROUP, hostname, count, BLOCK, false, key, XID::NewInGroup)
              .await?,
          );
        }
        Err(err.into())
      }
    }
  }
}
