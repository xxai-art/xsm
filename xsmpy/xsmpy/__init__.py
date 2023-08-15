#!/usr/bin/env python

from loguru import logger
from msgpack import unpackb, packb
from xsmpy.xsmpy import server_host_port, u64_bin  #noqa
from .pool import pool
import asyncio
from os import getenv

EMPTY = packb([])
BLOCK = 300


def log_err(func, args):

  def _(task):
    try:
      task.result()
    except Exception as e:
      logger.error("%s %s" % (func, args))
      logger.exception(e)

  return _


def ensure_future(func, *args):
  asyncio.ensure_future(func(*args)).add_done_callback(log_err(func, args))


async def stream_iter(stream, limit):
  for xid, [(id, args)] in await stream.xnext(limit):
    yield xid, id, args
    # li.append(f(stream, xid, server, id, args))

  # await gather(run_cost, li)

  while True:
    li = await stream.xpendclaim(limit)
    for retry, xid, id, args in li:
      logger.info(f'retry {retry} {xid} {id} {args}')
      if retry > 9:
        ensure_future(stream.xackdel, xid)
        continue
      # li.append(f(stream, xid, server, id, args))
      yield xid, id, args

    if not li:  # 有 pending 优先处理 pending
      break


def _func(func, server, stream):

  async def _(xid, id, args):
    id = unpackb(id)
    args = unpackb(args)
    r = await func(id, *args)
    await stream.xackdel(xid)
    if r:
      if len(r) == 2:
        next_args = EMPTY
      else:
        next_args = packb(r[2])
      await server.xadd(r[0], r[1], next_args)

  return _


async def _run(stream_name, func, duration):
  host_port = getenv('MQ_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(BLOCK, host, int(port), 'default',
                                  getenv('MQ_PASSWORD'))
  stream = server.stream(stream_name)
  func = _func(func, server, stream)
  await pool(func, BLOCK, duration, lambda limit: stream_iter(stream, limit))


def run(stream, func, duration=86400):
  asyncio.run(_run(stream, func, duration))
