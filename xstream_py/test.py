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

  for xid, [(id, args)] in await server.xnext("iaa", 32):
    print(xid, unpackb(id), unpackb(args))


asyncio.run(main())
