# !/home/hang/.environment/py3.6/bin/python
# -*- coding: utf-8 -*-

import ccxt.async as accxt
from data import Exchange, Market, Book
from util import throttle, get_state, update_state, State, STATE

import asyncio
import time
import threading
import json
import logging
import argparse
import threading
from pprint import pprint
from timeit import default_timer as timer

logger = logging.getLogger('fetcher')

CONF_PATH = './config.json'
CONF = {}
EXS = {}
MKTS = {}
LOOP = asyncio.get_event_loop()


def set_env():
    arg_parse = argparse.ArgumentParser(description='fetcher args')
    arg_parse.add_argument('-v', action='store_true', dest='verbose', default=False,
                           help='LOG LEVEL DOWN TO DEBUG')
    args = arg_parse.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    load_config()


def load_config():
    logger.info("loading config")

    global CONF_PATH, CONF
    with open(CONF_PATH) as file:
        CONF = json.load(file)
    if 'exchanges' not in CONF or 'markets' not in CONF:
        logger.error('incorrect config file')
        exit(1)


def setup_exchanges():
    logger.info("setuping exchanges through ccxt")

    global CONF, EXS
    exchanges = CONF.get('exchanges', None)
    if exchanges is None or not len(exchanges):
        logging.error('no exchanges from config.json')
        exit(1)

    for exs in exchanges:
        try:
            logger.info('setup %s', exs)
            exs_handler = getattr(accxt, exs)()
        except AttributeError:
            logger('incorrect exchanges name for ccxt')

        EXS[exs] = Exchange(exs, exs_handler)


async def load_markets():
    logger.info("loading markets")
    global EXS, MKTS
    tasks = [ex.handler.load_markets() for ex in EXS.values()]
    await asyncio.wait(tasks)

    for mkt in CONF['markets']:
        MKTS[mkt] = Market(mkt)

    for ex in EXS.values():
        all_markets = ex.handler.symbols
        for mkt in CONF['markets']:
            if mkt not in all_markets:
                logger.error("%s not supported by %s" % (mkt, ex.name))
                exit(1)

            book = Book(ex.name, mkt)
            MKTS[mkt].add_exchange(ex.name, book)
            ex.add_market(mkt, book)


async def cleanup():
    logger.info("clean up resources, ready to close")
    global EXS
    tasks = [ex.handler.close() for ex in EXS.values()]
    await asyncio.wait(tasks)


async def fetch_orderbook(exchange):
    logger.info('fetching order book from %s' % exchange.name)

    if exchange.handler.has['fetchOrderBook']:
        markets = exchange.market2book.keys()
        task2mkt = {asyncio.ensure_future(exchange.handler.fetch_order_book(mkt)): mkt
                    for mkt in markets}

        await asyncio.gather(*task2mkt.keys())

        for task, mkt in task2mkt.items():
            exchange.market2book[mkt].update(task.result())
            logger.info('received %s' % exchange.market2book[mkt])


async def fetch_all_orderbooks():
    logger.info('fetching all order books')
    global EXS
    return await asyncio.gather(*[fetch_orderbook(ex) for name, ex in EXS.items()])


if __name__ == '__main__':

    logger.info(
        'Starting fetcher'
    )

    set_env()
    setup_exchanges()
    LOOP.run_until_complete(load_markets())

    start = timer()
    LOOP.run_until_complete(fetch_all_orderbooks())
    print(timer() - start)

    try:
        while True:
            new_state = get_state()

            if new_state == State.STOPPED:
                time.sleep(1)
            elif new_state == State.RUNNING:
                tasks = [asyncio.ensure_future(fetch_all_orderbooks())]
                LOOP.run_until_complete(throttle(tasks, CONF['interval']))

            old_state = new_state
    except KeyboardInterrupt:
        logger.info('Got SIGINT, aborting ...')
    finally:
        LOOP.run_until_complete(cleanup())
        LOOP.close()
        logger.info('successfully closed')
