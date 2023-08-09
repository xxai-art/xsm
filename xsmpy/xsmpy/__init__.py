#!/usr/bin/env python

from sys import stderr
from msgpack import unpackb
from xsmpy.xsmpy import server_host_port
import asyncio
from os import getenv
import traceback


def _func(func):

  async def _(stream, xid, server, id, args):
    try:
      id = unpackb(id)
      args = unpackb(args)
      await func(server, id, *args)
      await stream.xackdel(xid)
    except Exception:
      traceback.print_exc()
      print(xid, id, args, file=stderr)

  return _


async def _run(stream, func):
  host_port = getenv('REDIS_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(host, int(port), 'default',
                                  getenv('REDIS_PASSWORD'))
  # server.xadd('iaa', 2, packb([]))
  stream = server.stream(stream)
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
