# !/home/hang/.environment/py3.6/bin/python
# -*- coding: utf-8 -*-

import ccxt.async as accxt
import book

import asyncio
import time
import threading
import json
import logging
import argparse
import threading
from pprint import pprint

logger = logging.getLogger('fetcher')

CONF_PATH = './config.json'
CONF = {}
EXS_HANDLERS = {}
BOOKS = {}
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

    global CONF, EXS_HANDLERS, BOOKS
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

        EXS_HANDLERS[exs] = exs_handler
        BOOKS[exs] = books.Book(exs)

async def load_markets():
    logger.info("loading markets")
    global EXS_HANDLERS
    tasks = [v.load_markets() for k, v in EXS_HANDLERS.items()]
    await asyncio.wait(tasks)


async def cleanup():
    logger.info("clean up resources, ready to close")
    global EXS_HANDLERS
    tasks = [v.close() for k, v in EXS_HANDLERS.items()]
    await asyncio.wait(tasks)


async def fetch_orderbook(exchange):
    global CONF
    if exchange.has['fetchOrderBook']:
        limit = 1
        orderbooks = await asyncio.gather(*[exchange.fetch_order_book(mkt, limit)
                                            for mkt in CONF['markets']])
        for index, ob in enumerate(orderbooks):
            bk = book.Book(ob, CONF['markets'][index])
            print(bk)           


async def fetch_all_orderbooks():
    logger.info('fetching order books')
    global EXS_HANDLERS
    return await asyncio.gather(*[fetch_orderbook(v) for k, v in EXS_HANDLERS.items()])


if __name__ == '__main__':

    logger.info(
        'Starting fetcher'
    )

    set_env()
    setup_exchanges()

    LOOP.run_until_complete(load_markets())
    rtn = LOOP.run_until_complete(fetch_all_orderbooks())
    pprint(rtn)

    #while True:
    #    LOOP.run_until_complete(fetch_all_order_book())
    #    LOOP.run_until_complete(record())

    LOOP.run_until_complete(cleanup())
    LOOP.close()
