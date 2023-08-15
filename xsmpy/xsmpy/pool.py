#!/usr/bin/env python
from loguru import logger
from asyncio import ensure_future, sleep
from time import time


def wrap(func):

  async def _(*args):
    try:
      begin = time()
      await func(*args)
      return time() - begin
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
      cost = await func(*i)
      if cost:
        n += 1
        sum_cost += cost
        if n >= limit and 0 == n % 2:
          speed = sum_cost / n
          limit = 1 + round(block / speed)
          logger.info('%.3f s/item %d limit' % (speed, limit))
          sum_cost /= 2
          n /= 2
    finally:
      runing -= 1

  while True:
    async for i in async_iter(limit):
      runing += 1
      task = _(i)
      if runing >= limit:
        await task
      else:
        ensure_future(task)

    diff = duration - (time() - startup)
    if diff <= 0:
      return
    logger.info('remain %.2f h' % (diff / 3600))
