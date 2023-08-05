#![feature(lazy_cell)]

use std::{cell::LazyCell, env, os::unix::ffi::OsStrExt, str::from_utf8};

use anyhow::Result;
use bytes::Bytes;
use fred::{
  interfaces::StreamsInterface,
  prelude::{RedisClient, RedisErrorKind::Unknown},
  types::XID,
};
use gethostname::gethostname;
use lazy_static::lazy_static;

lazy_static! {
  pub static ref HOSTNAME: String = gethostname().into_string().unwrap();
}

pub struct Client {
  c: RedisClient,
}

const BLOCK: Option<u64> = Some(60000);
const GROUP: &'static str = "C";

impl Client {
  pub async fn xnext(
    &self,
    key: impl AsRef<str>,
    count: u64, // 获取的数量
  ) -> Result<Vec<(Bytes, Vec<(String, Vec<(Bytes, Bytes)>)>)>> {
    let key = key.as_ref();
    let count = Some(count);
    let hostname = &*HOSTNAME;

    match self
      .c
      .xreadgroup(GROUP, hostname, count, BLOCK, false, key, XID::NewInGroup)
      .await
    {
      Ok(r) => Ok(r),
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
