import asyncio
from decimal import Decimal
import datetime
from collections import defaultdict
import functools
import logging

from cryptofeed.config import Config
from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import DeribitBookPostgres, DeribitTickerPostgres, DeribitTradePostgres
from cryptofeed.callback import DeribitTickerCallback
from cryptofeed.defines import BID, ASK, TRADES, L2_BOOK, PERPETURAL, OPTION, FUTURE, TICKER
from cryptofeed.exchanges import Deribit
from cryptofeed.log import get_logger
from cryptofeed.util.instrument import get_instrument_type

async def do_periodically_at(hour, minute, second, periodic_function):
    while True:
        t = datetime.datetime.today()
        future = datetime.datetime(t.year, t.month, t.day, hour, minute, second)
        if t >= future:
            LOG.info(f'{t} > {future}')
            future += datetime.timedelta(days=1)
        delay = (future - t).total_seconds()
        h = (int)(delay/3600)
        m = int((delay-h*3600)/60)
        s = (int)(delay%60)
        LOG.info(f'Waiting {h} hours {m} minutes {s} seconds to execute periodic function')
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
    PERPETURAL: [TICKER],
    OPTION: [TICKER, TRADES, L2_BOOK],
    FUTURE: [TICKER],
}

postgres_cfg = {'host': 'localhost', 'user': 'postgres', 'db': 'fs', 'pw': 'password'}
LOG = logging.getLogger('ingester')

def main():
    config = Config(config={'log': {'filename': 'ingester.log', 'level': 'DEBUG'}})
    get_logger('ingester', config.log.filename, config.log.level)
    f = FeedHandler(config=config)
    callbacks = {
        TICKER: DeribitTickerPostgres(**postgres_cfg, cache_size=1000), 
        TRADES: DeribitTradePostgres(**postgres_cfg),
        L2_BOOK: DeribitBookPostgres(**postgres_cfg, cache_size=1000)
    }
    deribit = Deribit(max_depth=2, subscription=get_new_subscription(), callbacks=callbacks)
    f.add_feed(deribit)
    f.run(start_loop=True, tasks=[do_periodically_at(8, 1, 1, functools.partial(subscribe_to_new_subscription, deribit))])
    
if __name__ == '__main__':
    main()
