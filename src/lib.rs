pub async fn xnext(
  &self,
  group: Bin,
  consumer: Bin,
  count: Option<u64>,
  block: Option<u64>,
  noack: bool,
  key: Bin,
) -> Result<Vec<(Val, Vec<(String, Vec<(Val, Val)>)>)>> {
  let key = &Into::<Box<[u8]>>::into(key)[..];
  let consumer = Into::<Box<[u8]>>::into(consumer);
  let consumer = from_utf8(consumer.as_ref())?;
  let group = Into::<Box<[u8]>>::into(group);
  let group = from_utf8(group.as_ref())?;
  match self
    .c
    .xreadgroup(group, consumer, count, block, noack, key, XID::NewInGroup)
    .await
  {
    Ok(r) => Ok(r),
    Err(err) => {
      if err.kind() == &Unknown && err.details().starts_with("NOGROUP ") {
        self
          .c
          .xgroup_create(key, group, XID::Manual("0".into()), true)
          .await?;
        return Ok(
          self
            .c
            .xreadgroup(group, consumer, count, block, noack, key, XID::NewInGroup)
            .await?,
        );
      }
      Err(err.into())
    }
  }
}
