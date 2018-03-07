
class Market(object):
    def __init__(self, name):
        self.name = name
        self.exs2book = {}

    def add_exchange(self, exchange, book):
        if exchange not in self.exs2book:
            self.exs2book[exchange] = book


class Exchange(object):
    def __init__(self, name, handler):
        self.name = name
        self.market2book = {}
        self.handler = handler

    def add_market(self, market, book):
        if market not in self.market2book:
            self.market2book[market] = book


class Book(object):
    def __init__(self, exchange, market):
        self.exchange = exchange
        self.market = market
        self.bid1price = 0
        self.bid1vol = 0
        self.ask1price = 0
        self.ask1vol = 0
        self.datetime = None

    def __str__(self):
        return 'exchange:%s market:%s bid1p:%d ask1p:%d' % (
            self.exchange, self.market, self.bid1price, self.ask1price)

    def __repr__(self):
        return 'exchange:%s market:%s bid1p:%d ask1p:%d' % (
            self.exchange, self.market, self.bid1price, self.ask1price)

    def update(self, orderbook):
        self.bid1price = orderbook['bids'][0][0]
        self.bid1vol = orderbook['bids'][0][1]
        self.ask1price = orderbook['asks'][0][0]
        self.ask1vol = orderbook['asks'][0][1]
        self.datetime = orderbook['datetime']
