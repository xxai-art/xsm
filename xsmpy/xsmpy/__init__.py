#!/usr/bin/env python

from sys import stderr
from msgpack import unpackb, packb
from xsmpy.xsmpy import server_host_port
import asyncio
from os import getenv
import traceback

EMPTY = packb([])


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
    except Exception:
      traceback.print_exc()
      print(xid, id, args, file=stderr)

  return _


async def _run(stream_name, func):
  host_port = getenv('REDIS_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(host, int(port), 'default',
                                  getenv('REDIS_PASSWORD'))
  stream = server.stream(stream_name)
  limit = 32
  while True:
    for retry, xid, id, args in await stream.xpendclaim(limit):
      if retry > 6:
        await stream.xackdel(xid)
        continue
      await func(stream, xid, server, id, args)

    for xid, [(id, args)] in await stream.xnext(limit):
      await func(stream, xid, server, id, args)


def run(stream, func):
  asyncio.run(_run(stream, _func(func)))
