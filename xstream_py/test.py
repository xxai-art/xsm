#!/usr/bin/env python

from msgpack import unpackb
from xstream_py import server_host_port
import asyncio
from os import getenv


async def main():
  host_port = getenv('REDIS_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(host, int(port), 'default',
                                  getenv('REDIS_PASSWORD'))

  stream = server.stream("iaa")
  limit = 32
  while True:
    for retry, task_id, id, args in await stream.xpendclaim(limit):
      id = unpackb(id)
      args = unpackb(args)
      await stream.xackdel(task_id)
      print(f"retry {retry} {task_id} {id} {args}")

    print('xnext')
    for xid, [(id, args)] in await stream.xnext(limit):
      print(xid, unpackb(id), unpackb(args))


asyncio.run(main())
