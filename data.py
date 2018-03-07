
class Market(object):
    def __init__(self, name):
        self.name = name
        self.exs2book = {}

    def add_exchange(self, exchange, book):
        if exchange not in self.exs2book:
            self.exs2book[exchange] = book

    def trans_bson(self):
        if len(self.exs2book) == 0:
            logger.error('transfer empty market')
            exit(1)

        db_item = {}
        for ex_name, book in self.exs2book.items():
            db_item[ex_name] = book.trans_bson()
        return db_item


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
        print(self.datetime)

    def trans_bson(self):
        return {
            'bid1p': self.bid1price,
            'bid1v': self.bid1vol,
            'ask1p': self.ask1price,
            'ask1v': self.ask1vol
        }
