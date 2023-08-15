#!/usr/bin/env python
from loguru import logger
from asyncio import ensure_future, sleep
from time import time


def wrap(func):

  async def _(*args):
    try:
      await func(*args)
      return True
    except Exception as e:
      logger.error("%s %s" % (func, args))
      logger.exception(e)
      await sleep(3)
      return

  return _


async def pool(func, block, duration, async_iter):
  duration -= 5 * block  # 提前结束避免超过
  limit = 2
  func = wrap(func)
  startup = time()

  n = sum_cost = runing = 0

  async def _(i):
    nonlocal n, sum_cost, limit, runing
    try:
      if await func(*i):
        n += 1
    finally:
      runing -= 1

  while True:
    begin = time()

    async for i in async_iter(limit):
      runing += 1
      task = _(i)
      if runing >= limit:
        await task
      else:
        ensure_future(task)

    now = time()

    diff = duration - (now - startup)
    if diff <= 0:
      return

    if n:
      speed = (now - begin) / n
      limit = 1 + round(block / speed)
      logger.info('%.3f s/item %d limit remain %.2f h' %
                  (speed, limit, diff / 3600))
      n = 0
