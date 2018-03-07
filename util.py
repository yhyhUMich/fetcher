import enum
import asyncio
import logging
from wrapt import synchronized
from timeit import default_timer as timer

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


async def throttle(tasks, interval):
    start = timer()
    result = await asyncio.gather(*tasks)
    end = timer()
    duration = max(interval - (end - start), 0.0)

    logger.info('Throttling tasks for %.2f seconds', duration)
    await asyncio.sleep(duration)

    return result
