#!/usr/bin/env python

from xsmpy import run


async def task(id, *args):
  print(id, args)
  # return 'clip', id
  return


run('test', task)
