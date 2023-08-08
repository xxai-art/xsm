#!/usr/bin/env python

from xstream_py import server_host_port
import asyncio
from os import getenv


async def main():
  host_port = getenv('REDIS_HOST_PORT')
  host, port = host_port.split(':')
  server = await server_host_port(host, int(port), 'default',
                                  getenv('REDIS_PASSWORD'))

  print(await server.xnext("iaa", 32))


asyncio.run(main())
