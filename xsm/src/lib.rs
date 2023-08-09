#![feature(lazy_cell)]
#![feature(const_option)]

use std::{marker::Send, ops::Deref};

use anyhow::Result;
use bytes::Bytes;
pub use fred::{
  self,
  interfaces::{ClientLike, StreamsInterface},
  prelude::{
    PerformanceConfig, ReconnectPolicy, RedisClient, RedisConfig, RedisErrorKind::Unknown,
    ServerConfig,
  },
  types::XID,
};
use fred::{interfaces::FunctionInterface, prelude::RedisError, types::MultipleOrderedPairs};
use gethostname::gethostname;
use lazy_static::lazy_static;
use rand::Rng;
pub use xxai_msgpacker;
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

#[derive(Clone)]
pub struct Stream {
  c: Client,
  name: String,
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

  pub fn stream(&self, name: String) -> Stream {
    Stream {
      c: self.clone(),
      name,
    }
  }

  pub async fn xadd<V: TryInto<MultipleOrderedPairs> + Send>(
    &self,
    stream: impl AsRef<str>,
    val: V,
  ) -> Result<()>
  where
    V::Error: Into<RedisError> + Send,
  {
    Ok(
      self
        .c
        .xadd(stream.as_ref(), false, Some(()), XID::Auto, val)
        .await?,
    )
  }
}

impl Stream {
  pub async fn xclean(&self) -> Result<()> {
    self
      .c
      .fcall::<Option<Bytes>, _, _, _>(
        "xconsumerclean",
        vec![&self.name, &self.c.group],
        vec![604800000],
      )
      .await?;
    Ok(())
  }

  pub async fn xackdel(&self, task_id: impl AsRef<str>) -> Result<()> {
    self
      .c
      .fcall::<Option<Bytes>, _, _, _>(
        "xackdel",
        vec![&self.name, &self.c.group],
        vec![task_id.as_ref()],
      )
      .await?;
    Ok(())
  }

  pub async fn xpendclaim(
    &self,
    limit: u32,
  ) -> Result<Option<Vec<(u64, u64, u64, Vec<u8>, Vec<u8>)>>> {
    if 0 == rand::thread_rng().gen_range(0..7 * 24 * 60) {
      self.xclean().await?;
    }
    if let Some(r) = self
      .c
      .fcall::<Option<Bytes>, _, _, _>(
        "xpendclaim",
        vec![&self.name, &self.c.group, &HOSTNAME],
        vec![self.c.pending, limit],
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
    Ok(None)
  }

  pub async fn xnext(
    &self,
    count: u64, // 获取的数量
  ) -> Result<Option<Vec<(String, Vec<(Bytes, Bytes)>)>>> {
    let count = Some(count);
    let hostname = &*HOSTNAME;

    match self
      .c
      .xreadgroup::<Vec<(Bytes, _)>, _, _, _, _>(
        &self.c.group,
        hostname,
        count,
        self.c.block,
        false,
        &self.name,
        XID::NewInGroup,
      )
      .await
    {
      Ok(mut r) => Ok(if let Some(r) = r.pop() { r.1 } else { None }),
      Err(err) => {
        if err.kind() == &Unknown && err.details().starts_with("NOGROUP ") {
          self
            .c
            .xgroup_create(&self.name, &self.c.group, XID::Manual("0".into()), true)
            .await?;
          return Ok(
            self
              .c
              .xreadgroup(
                &self.c.group,
                hostname,
                count,
                self.c.block,
                false,
                &self.name,
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
