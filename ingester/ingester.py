import asyncio
from decimal import Decimal
import datetime
from collections import defaultdict
import functools

from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import DeribitBookPostgres, DeribitTickerPostgres, DeribitTradePostgres
from cryptofeed.callback import DeribitTickerCallback
from cryptofeed.defines import BID, ASK, TRADES, L2_BOOK, PERPETURAL, OPTION, FUTURE, TICKER
from cryptofeed.exchanges import Deribit
from cryptofeed.util.instrument import get_instrument_type

async def trade(feed, symbol, order_id, timestamp, side, amount, price, receipt_timestamp, **kwargs):
    assert isinstance(timestamp, float)
    assert isinstance(side, str)
    assert isinstance(amount, Decimal)
    assert isinstance(price, Decimal)
    print(f"Timestamp: {get_time_from_timestamp(timestamp * 1000)} Rx: {get_time_from_timestamp(receipt_timestamp * 1000)} (+{receipt_timestamp * 1000 - timestamp * 1000}) Feed: {feed} Pair: {symbol} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")

async def ticker(feed, symbol, bid, bid_amount, ask, ask_amount, timestamp, receipt_timestamp, bid_iv, ask_iv, delta, gamma, rho, theta, vega, mark_price, mark_iv, underlying_index, underlying_price):
    print(f'Timestamp: {get_time_from_timestamp(timestamp * 1000)} Feed: {feed} Pair: {symbol} Bid: {bid} Bid amount: {bid_amount} Ask: {ask} Ask amount: {ask_amount} Bid iv: {bid_iv} Ask iv: {ask_iv} Delta: {delta} Gamma: {gamma} Rho: {rho} Theta: {theta} Vega: {vega} Mark price: {mark_price} Mark iv: {mark_iv}')

async def perp_ticker(feed, symbol, bid, bid_amount, ask, ask_amount, timestamp, receipt_timestamp):
    print(f'Timestamp: {get_time_from_timestamp(timestamp * 1000)} Feed: {feed} Pair: {symbol} Bid: {bid} Bid amount: {bid_amount} Ask: {ask} Ask amount: {ask_amount}')

async def book(feed, symbol, book, timestamp, receipt_timestamp):
    print(f'Timestamp: {get_time_from_timestamp(timestamp * 1000)} Feed: {feed} Pair: {symbol} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])} {book}')

async def do_periodically_at(hour, minute, second, periodic_function):
    while True:
        t = datetime.datetime.today()
        future = datetime.datetime(t.year, t.month, t.day, hour, minute, second)
        if t >= future:
            print(f'{t} > {future}')
            future += datetime.timedelta(days=1)
        delay = (future - t).total_seconds()
        h = (int)(delay/3600)
        m = int((delay-h*3600)/60)
        s = (int)(delay%60)
        print(f'Waiting {h} hours {m} minutes {s} seconds to execute periodic function')
        await asyncio.sleep(delay)
        await periodic_function()

def get_new_subscription():
    symbols = Deribit.get_instruments()
    new_subscription = defaultdict(set)
    for symbol in symbols:
        for feed_type in subscriptions[get_instrument_type(symbol)]:
            new_subscription[feed_type].add(symbol)
    return new_subscription

async def subscribe_to_new_subscription(deribit):
    await deribit.update_subscription(subscription=get_new_subscription())
    # wait a little to let the clock tick on
    await asyncio.sleep(5)

def get_time_from_timestamp(timestamp):
    s, ms = divmod(timestamp, 1000)
    return '%s.%03d' % (datetime.datetime.utcfromtimestamp(s).strftime('%H:%M:%S'), ms)

subscriptions = {
    # PERPETURAL: [TICKER],
    PERPETURAL: [],
    OPTION: [TICKER, TRADES, L2_BOOK],
    # FUTURE: [TICKER],
    FUTURE: []
}

postgres_cfg = {'host': '127.0.0.1', 'user': 'postgres', 'db': 'fs', 'pw': None}

def main():
    f = FeedHandler()
    callbacks = {
        TICKER: DeribitTickerPostgres(**postgres_cfg, cache_size=1000), 
        TRADES: DeribitTradePostgres(**postgres_cfg),
        L2_BOOK: DeribitBookPostgres(**postgres_cfg, cache_size=1000)
    }
    # deribit = Deribit(max_depth=1, subscription=get_new_subscription(), callbacks={L2_BOOK: book})#, TRADES: trade, TICKER: DeribitTickerCallback(callbacks={OPTION: ticker, PERPETURAL: perp_ticker})})
    deribit = Deribit(max_depth=2, subscription=get_new_subscription(), callbacks=callbacks)
    f.add_feed(deribit)
    f.run(start_loop=True, tasks=[do_periodically_at(8, 1, 1, functools.partial(subscribe_to_new_subscription, deribit))])
    
if __name__ == '__main__':
    main()
