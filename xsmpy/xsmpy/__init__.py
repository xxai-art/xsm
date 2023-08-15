#!/usr/bin/env python

from loguru import logger
from datetime import datetime
from msgpack import unpackb, packb
from xsmpy.xsmpy import server_host_port
from xsmpy.xsmpy import u64_bin  #noqa
import asyncio
from os import getenv

EMPTY = packb([])
BLOCK = 300


def now():
  return datetime.now().timestamp()


def _func(func):

  async def _(stream, xid, server, id, args):
    try:
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
    except Exception as e:
      logger.exception(e)
      await asyncio.sleep(3)

  return _


async def gather(run_cost, li):

  n = 0

  begin = now()
  for pos, i in enumerate(await asyncio.gather(*li, return_exceptions=True)):
    if isinstance(i, Exception):
      logger.error(li[pos])
      logger.exception(i)
    else:
      n += 1

  if n:
    run_cost[0] += n
    run_cost[1] += (now() - begin)


def log_err(func, args):

  def _(task):
    try:
      task.result()
    except Exception as e:
      logger.error("%s %s" % (func, args))
      logger.exception(e)

  return _


def ensure_future(func, *args):
  task = asyncio.ensure_future(func(*args))
  task.add_done_callback(log_err(func, args))


async def _run(stream_name, func, duration):
  begin = now()
  run_cost = [0, 0]
  f = _func(func)
  host_port = getenv('MQ_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(BLOCK, host, int(port), 'default',
                                  getenv('MQ_PASSWORD'))
  stream = server.stream(stream_name)
  limit = 1
  while True:
    li = []
    for xid, [(id, args)] in await stream.xnext(limit):
      li.append(f(stream, xid, server, id, args))

    await gather(run_cost, li)

    while True:
      li = []
      for retry, xid, id, args in await stream.xpendclaim(limit):
        logger.info(f'retry {retry} {xid} {id} {args}')
        if retry > 9:
          ensure_future(stream.xackdel, xid)
          continue
        li.append(f(stream, xid, server, id, args))

      await gather(run_cost, li)

      [run, cost] = run_cost
      if run:
        speed = cost / run
        limit = round(BLOCK / speed) + 1
        if run > limit:
          run_cost[0] = run / 2
          run_cost[1] = cost / 2
        logger.info('%.3f s/item %d limit' % (speed, limit))

      remain = duration - now() + begin
      if remain < 0:
        return
      logger.info(f'remain {remain/60:.1f} min')
      if len(li) == 0:  # 有 pending 优先处理 pending
        break


def run(stream, func, duration=86000):
  asyncio.run(_run(stream, func, duration))
