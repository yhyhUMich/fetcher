# !/home/hang/.environment/py3.6/bin/python
# -*- coding: utf-8 -*-

import ccxt.async as accxt
import ccxt
from data import Exchange, Market, Book
from database import db_proc_start
from util import markets2bson, throttle, get_state, update_state, State, STATE

import asyncio
import time
import threading
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import argparse
import os
import signal
from pprint import pprint
from timeit import default_timer as timer
from multiprocessing import Process, Queue

logger = logging.getLogger('fetcher')

CONF_PATH = './config.json'
CONF = {}
EXS = {}
MKTS = {}
LOOP = asyncio.get_event_loop()
QUE = Queue()
DBP = None


def ask_exit():
    logger.info("canceling all tasks")
    for task in asyncio.Task.all_tasks():
        task.cancel()


def set_env():
    logger.info("seting environment")

    arg_parse = argparse.ArgumentParser(description='fetcher args')
    arg_parse.add_argument('-v', action='store_true', dest='verbose', default=False,
                           help='LOG LEVEL DOWN TO DEBUG')
    args = arg_parse.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    log_path = "./main.log"
    fh = TimedRotatingFileHandler(log_path, 'H', 6, 5)
    fh.setLevel(logging.INFO)
    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    load_config()

    for sig in (signal.SIGTERM, ):
        LOOP.add_signal_handler(sig, ask_exit)


def load_config():
    logger.info("loading config")

    global CONF_PATH, CONF
    with open(CONF_PATH) as file:
        CONF = json.load(file)
    if 'exchanges' not in CONF:
        logger.error('incorrect config file')
        exit(1)


def setup_db():
    logger.info('start db process')

    global CONF, DBP
    DBP = Process(target=db_proc_start, args=(QUE, CONF))
    DBP.start()


def setup_exchanges():
    logger.info("setuping exchanges through ccxt")

    global CONF, EXS
    exchanges = CONF.get('exchanges', None)
    if exchanges is None or not len(exchanges):
        logging.error('no exchanges from config.json')
        exit(1)

    for exs in exchanges.keys():
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

    for mkts in CONF['exchanges'].values():
        for mkt in mkts:
            if mkt not in MKTS:
                MKTS[mkt] = Market(mkt)

    for ex, mkts in CONF['exchanges'].items():
        all_markets = EXS[ex].handler.symbols
        for mkt in mkts:
            if mkt not in all_markets:
                logger.error("%s not supported by %s" % (mkt, ex))
                exit(1)

            book = Book(ex, mkt)
            MKTS[mkt].add_exchange(ex, book)
            EXS[ex].add_market(mkt, book)


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
            # logger.info('received %s' % exchange.market2book[mkt])


async def fetch_all_orderbooks():
    logger.info('fetching all order books')
    global EXS
    await asyncio.gather(*[fetch_orderbook(ex) for name, ex in EXS.items()])


if __name__ == '__main__':

    set_env()

    logger.info(
        'Starting fetcher, PID: %s ' % os.getpid()
    )

    setup_db()
    setup_exchanges()

    LOOP.run_until_complete(load_markets())

    try:
        call = markets2bson(MKTS, QUE)

        while True:
            tasks = [asyncio.ensure_future(fetch_all_orderbooks())]
            try:
                LOOP.run_until_complete(throttle(tasks, [call], CONF['interval']))
            except Exception as e:
                logger.warn("Error : %s " % e)
                ask_exit()
    except KeyboardInterrupt:
        logger.info('Got SIGINT, aborting ...')
    finally:
        ask_exit()
        LOOP.run_until_complete(cleanup())
        LOOP.close()
        DBP.terminate()
        logger.info('successfully closed')
