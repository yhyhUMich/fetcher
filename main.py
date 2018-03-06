# !/home/hang/.environment/py3.6/bin/python
# -*- coding: utf-8 -*-

import ccxt.async as accxt

import asyncio
import time
import threading
import json
import logging
from pprint import pprint


logger = logging.getLogger('fetcher')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

CONF_PATH = './config.json'
CONF = {}
EXS_HANDLERS = {}


def load_config():
    logger.info("loading config")

    global CONF_PATH
    global CONF
    with open(CONF_PATH) as file:
        CONF = json.load(file)
    if 'exchanges' not in CONF or 'markets' not in CONF:
        logger.error('incorrect config file')
        exit(1)


def setup_exchanges():
    logger.info("setuping exchanges through ccxt")

    global CONF
    global EXS_HANDLERS
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


async def cleanup():
    logger.info("clean up resources, ready to close")
    global EXS_HANDLERS
    for k, v in EXS_HANDLERS.items():
        await v.close()
        logger.info('close %s', k)

if __name__ == '__main__':

    logger.info(
        'Starting fetcher'
    )

    load_config()
    setup_exchanges()
    pprint(EXS_HANDLERS)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([cleanup()]))
    loop.close()
