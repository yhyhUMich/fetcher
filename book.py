
class Book(object):

    def __init__(self, exchange):
        self.exchange = exchange
    def __init__(self, orderbook, market):
        self.market = market
        self.exchange = ''
        self.bid1price = orderbook['bids'][0][0]
        self.bid1vol = orderbook['bids'][0][1]
        self.ask1price = orderbook['asks'][0][0]
        self.ask1vol = orderbook['asks'][0][1]
        self.datetime = orderbook['datetime']

    def __str__(self):
        return 'market:%s bid1p:%d ask1p:%d' % (self.market, self.bid1price, self.ask1price)
    