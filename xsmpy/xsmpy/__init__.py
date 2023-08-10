#!/usr/bin/env python

from loguru import logger
from datetime import datetime
from msgpack import unpackb, packb
from xsmpy.xsmpy import server_host_port
import asyncio
from os import getenv

EMPTY = packb([])


def now():
  return datetime.now().timestamp()


def _func(func, run_cost):

  async def _(stream, xid, server, id, args):
    try:
      begin = now()
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
      run_cost[1] += now() - begin
    except Exception as e:
      logger.exception(e)

  return _


def callback(f):
  e = f.exception()
  if e is not None:
    logger.exception(e)


def gather(li):
  asyncio.gather(*li).add_done_callback(callback)


async def _run(stream_name, func, duration):
  begin = now()
  run_cost = [0, 0]
  f = _func(func, run_cost)
  host_port = getenv('MQ_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(host, int(port), 'default',
                                  getenv('MQ_PASSWORD'))
  stream = server.stream(stream_name)
  limit = 1
  while True:
    li = []
    for xid, [(id, args)] in await stream.xnext(limit):
      li.append(f(stream, xid, server, id, args))

    gather(li)

    li = []
    for retry, xid, id, args in await stream.xpendclaim(limit):
      logger.info(f'retry {retry} {xid} {id} {args}')
      if retry > 9:
        li.append(stream.xackdel(xid))
        continue
      li.append(f(stream, xid, server, id, args))

    gather(li)

    [run, cost] = run_cost
    if run:
      speed = cost / run
      limit = max(1, round(((60 / speed) + limit * 7) / 8))
      if run > limit:
        run_cost[0] = run / 2
        run_cost[1] = cost / 2
      logger.info('limit %d %.3f s/item' % (limit, speed))

    remain = now() - begin
    if remain > duration:
      return
    logger.info(f'remain {remain} s')


def run(stream, func, duration=86280):
  asyncio.run(_run(stream, func, duration))
