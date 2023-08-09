#!/usr/bin/env python

from xsmpy import run


async def task(sredis, id, *args):
  print(id, args)
  return


run('iaa', task)
