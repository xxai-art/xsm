#!/usr/bin/env python

from xstream_py import sum_as_string, sleep_for
import asyncio

async def main():
  print('sleep')
  await sleep_for()
  print(sum_as_string(1, 3))

asyncio.run(main())

