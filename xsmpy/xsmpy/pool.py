#!/usr/bin/env python
from loguru import logger
from asyncio import Semaphore
from time import time


async def wrap(sem, func):

  def _(*args):
    try:
      async with sem:
        begin = time()
        await func(*args)
        return time() - begin
    except Exception as e:
      logger.error("%s %s" % (func, args))
      logger.exception(e)
      return

  return _


async def pool(block, duration, func, async_iter):
  duration -= 5 * block  # 提前结束避免超过
  limit = 2
  sem = Semaphore(limit)
  func = wrap(sem, func)
  n = 0
  sum_cost = 0
  startup = time()
  while True:
    time()
    async for i in async_iter(limit):
      cost = await func(i)
      if cost:
        n += 1
        sum_cost += cost
        if n >= limit:
          speed = sum_cost / n
          limit = 1 + round(block / speed)
          logger.info('%.3f s/item %d limit' % (speed, limit))
          sem._value = limit
          sum_cost /= 2
          n /= 2
    diff = duration - (time() - startup)
    if diff <= 0:
      return
    logger.info('remain hour %.2f', diff / 3600)
