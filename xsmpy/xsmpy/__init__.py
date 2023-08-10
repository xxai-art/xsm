#!/usr/bin/env python

from loguru import logger
from datetime import datetime
from msgpack import unpackb, packb
from xsmpy.xsmpy import server_host_port
import asyncio
from os import getenv

EMPTY = packb([])


def _func(func, run_cost):

  async def _(stream, xid, server, id, args):
    try:
      begin = datetime.now().timestamp()
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
      run_cost[0] += 1
      run_cost[1] += datetime.now().timestamp() - begin
    except Exception as e:
      logger.exception(e)

  return _


async def _run(stream_name, func):
  run_cost = [0, 0]
  f = _func(func, run_cost)
  host_port = getenv('MQ_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(host, int(port), 'default',
                                  getenv('MQ_PASSWORD'))
  stream = server.stream(stream_name)
  limit = 1
  while True:
    for xid, [(id, args)] in await stream.xnext(limit):
      await f(stream, xid, server, id, args)

    for retry, xid, id, args in await stream.xpendclaim(limit):
      logger.info(f'retry {retry} {xid} {id} {args}')
      if retry > 9:
        await stream.xackdel(xid)
        continue
      await f(stream, xid, server, id, args)

    [run, cost] = run_cost
    if run:
      speed = cost / run
      limit = max(1, round(((60 / speed) + limit * 7) / 8))
      if run > limit:
        run_cost[0] = run / 2
        run_cost[1] = cost / 2
      logger.info('limit %d %.2f ms/item' % (limit, speed * 1000))


def run(stream, func):
  asyncio.run(_run(stream, func))
