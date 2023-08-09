#!/usr/bin/env python

from xsmpy import run


async def task(redis, id, *args):
  print(id, args)
  return


run(task)
