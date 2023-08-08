#![feature(lazy_cell)]
#![feature(const_option)]

use std::ops::Deref;

use anyhow::Result;
use bytes::Bytes;
use fred::interfaces::FunctionInterface;
pub use fred::{
  self,
  interfaces::{ClientLike, StreamsInterface},
  prelude::{
    PerformanceConfig, ReconnectPolicy, RedisClient, RedisConfig, RedisErrorKind::Unknown,
    ServerConfig,
  },
  types::XID,
};
use gethostname::gethostname;
use lazy_static::lazy_static;
use xxai_msgpacker::unpack_array;

lazy_static! {
  pub static ref HOSTNAME: String = gethostname().into_string().unwrap();
}

#[derive(Clone)]
pub struct Client {
  c: RedisClient,
  block: Option<u64>,
  pending: u32,
  group: String,
}

impl Deref for Client {
  type Target = RedisClient;

  fn deref(&self) -> &Self::Target {
    &self.c
  }
}

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
    block: Option<u64>,
    pending: u32,
    group: impl Into<String>,
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
    Ok(Self {
      c: client,
      block,
      pending,
      group: group.into(),
    })
  }

  pub async fn xpendclaim(
    &self,
    stream: impl Into<String>,
    limit: u32,
  ) -> Result<Option<Vec<(u64, u64, u64, Vec<u8>, Vec<u8>)>>> {
    if let Some(r) = self
      .fcall::<Option<Bytes>, _, _, _>(
        "xpendclaim",
        vec![stream.into(), self.group.clone(), HOSTNAME.to_string()],
        vec![self.pending, limit],
      )
      .await?
    {
      if !r.is_empty() {
        let b0 = r[0];
        if b0 < 6 {
          let b0 = b0 as usize;
          let end = b0 + 1;
          let mut bin_len = [0u8; 8];
          bin_len[..b0].copy_from_slice(&r[1..end]);
          let mut begin = end;
          let mut end = begin + usize::from_le_bytes(bin_len);

          let (_, li): (_, Vec<u64>) = unpack_array(&r[begin..end])?;

          let li = li
            .chunks(5)
            .map(|t| {
              let [retry, t0, t1, klen, vlen] = t else {
                unreachable!()
              };
              begin = end;
              end += (*klen) as usize;
              let key = r[begin..end].to_vec();
              begin = end;
              end += (*vlen) as usize;
              let val = r[begin..end].to_vec();
              (*retry, *t0, *t1, key, val)
            })
            .collect::<Vec<_>>();

          return Ok(Some(li));
        }
      }
    }
    return Ok(None);
  }

  pub async fn xnext(
    &self,
    stream: impl AsRef<str>,
    count: u64, // 获取的数量
  ) -> Result<Option<Vec<(String, Vec<(Bytes, Bytes)>)>>> {
    let stream = stream.as_ref();
    let count = Some(count);
    let hostname = &*HOSTNAME;

    match self
      .c
      .xreadgroup::<Vec<(Bytes, _)>, _, _, _, _>(
        &self.group,
        hostname,
        count,
        self.block,
        false,
        stream,
        XID::NewInGroup,
      )
      .await
    {
      Ok(mut r) => Ok(if let Some(r) = r.pop() { r.1 } else { None }),
      Err(err) => {
        if err.kind() == &Unknown && err.details().starts_with("NOself.group ") {
          self
            .c
            .xgroup_create(stream, &self.group, XID::Manual("0".into()), true)
            .await?;
          return Ok(
            self
              .c
              .xreadgroup(
                &self.group,
                hostname,
                count,
                self.block,
                false,
                stream,
                XID::NewInGroup,
              )
              .await?,
          );
        }
        Err(err.into())
      }
    }
  }
}
