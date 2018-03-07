import enum
import asyncio
import logging
from wrapt import synchronized
from timeit import default_timer as timer
import datetime

from data import Market
from database import DBMange

logger = logging.getLogger('fetcher')


class State(enum.Enum):
    RUNNING = 0
    STOPPED = 1

STATE = State.RUNNING


@synchronized
def update_state(state):
    global _STATE
    STATE = state


@synchronized
def get_state():
    return STATE


def markets2bson(mkts, que):
    mkts = mkts
    que = que

    def inner():
        markets = {}
        for name, mkt in mkts.items():
            markets[name] = mkt.trans_bson()
        db_item = {
            'markets': markets,
            'datatime': 'test'
        }

        que.put(db_item)

    return inner


async def throttle(tasks, calls, interval):
    start = timer()
    await asyncio.gather(*tasks)

    [call() for call in calls]

    end = timer()
    duration = max(interval - (end - start), 0.0)
    logger.info('Throttling tasks for %.2f seconds', duration)
    await asyncio.sleep(duration)
