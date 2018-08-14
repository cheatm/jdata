from itertools import product
from jaqsd.kline.conf import DAILY_TAG, BAR_TAG, STATUS_TAG
from datetime import datetime
from jaqsd import api
import logging
import pandas as pd


class Task(object):

    def create(self, symbol, date):
        pass
    
    def select(self, symbols=None, dates=None, tag=None, value=0, sort="date"):
        pass
    
    def fill(self, symbol, date, tag, value):
        pass
    
    def recent(self):
        pass

    def check(self, symbol, date, tag=None):
        pass
    
    def clear(self, symbol, date):
        pass


class KlineStorage(object):

    def write(self, data):
        return []
    
    def check(self, symbol, date):
        return False
    

def today():
    date = datetime.today()
    return date.year*10000 + date.month*100 + date.day


class TaskIndex(object):

    def __init__(self, task):
        assert isinstance(task, Task)
        self.task = task

    def create(self, symbols=None, start=None, end=None):
        if end is None:
            end = today()
        if start is None:
            start = self.task.recent() or end
        trade_dates = api.get_trade_days(start, end).apply(int)
        if not symbols:
            symbols = api.get_A_stocks()
        suc, dup, fail = 0, 0, 0
        for date, symbol in product(trade_dates, symbols):
            try:
                r = self.task.create(symbol, date)
            except Exception as e:
                fail += 1
                logging.error("Kline create index | %s | %s | %s", symbol, date, e)
            else:
                logging.debug("Kline create index | %s | %s | %s", symbol, date, r)
                if r:
                    suc += 1
                else:
                    dup += 1
        logging.warning("Kline create index | OK: %s | DUP: %s | FAIL: %s", suc, dup, fail)
    

class DailyTaskExecutor(object):

    TAG = DAILY_TAG

    def __init__(self, task, storage):
        assert isinstance(task, Task)
        assert isinstance(storage, KlineStorage)
        self.task = task
        self.storage = storage
    
    def handle(self, data):
        self.fill_status(data)
        return self.storage.write(data)
    
    def fill_status(self, data):
        status = data.set_index(["symbol", "trade_date"])["trade_status"]
        trade = 0
        stop = 0
        for names, value in status.iteritems():
            symbol, date = names
            if value == "交易":
                self.task.fill(symbol, date, STATUS_TAG, 1) 
                trade += 1
            else:
                self.task.fill(symbol, date, STATUS_TAG, 0)
                stop += 1
        logging.warning("kline fill status | trade: %s | stop: %s", trade, stop)

    def _handle_daily(self, date, symbols):
        if len(symbols) <=4:
            names = ",".join(symbols)
        else:
            names = "%s,%s...%s,%s" % (symbols[0], symbols[1], symbols[-2], symbols[-1])
        try:
            data = self.query(",".join(symbols), date, date)
            if len(data.index):
                result = self.handle(data)
            else:
                logging.warning("kline load | %s | %s | empty", names, date)
                return [(symbol, date, -1) for symbol in symbols] 
        except Exception as e:
            logging.error("kline load | %s | %s | %s", names, date, e)
            return []
        else:
            logging.warning("kline load | %s | %s | ok", names, date)
            return result
    
    def _handle_symbol(self, symbol, dates):
        start, end = dates[0], dates[-1]
        try:
            data = self.query(symbol, start, end)
            if len(data.index):
                result = self.handle(data)
            else:
                logging.warning("kline load | %s | %s | %s | empty", symbol, start, end)
                return [(symbol, date, -1) for date in dates]
        except Exception as e:
            logging.error("kline load | %s | %s | %s | %s", symbol, start, end, e)
            return []
        else:
            logging.warning("kline load | %s | %s | %s | ok", symbol, start, end)
            return result 

    def _load_by_symbol(self, frame):
        for symbol, series in frame.iteritems():
            dates = series.dropna().index
            yield from self._handle_symbol(symbol, dates)

    def _load_by_dates(self, frame):
        for date, series in frame.iterrows():
            symbols = list(series.dropna().index)
            yield from self._handle_daily(date, symbols)

    def query(self, symbols, start, end):
        return api.daily(symbols, start, end)

    def publish(self, symbols=None, dates=None):
        tasks = list(self.task.select(symbols, dates, self.TAG))
        if len(tasks):
            frame = pd.DataFrame(tasks).pivot(1, 0, 2)
        else:
            return
        shape = frame.shape
        if shape[0] >= shape[1]:
            iterable = self._load_by_symbol(frame)
        else:
            iterable = self._load_by_dates(frame)
        for symbol, date, tag in iterable:
            self.task.fill(symbol, date, self.TAG, tag)
        

    def check(self, symbols=None, dates=None, tag=0):
        tasks = list(self.task.select(symbols, dates, self.TAG, tag, "symbol"))
        for symbol, date, _tag in tasks:
            r = self.storage.check(symbol, date)
            if r != _tag:
                self.task.fill(symbol, date, self.TAG, r)
            logging.warning("kline check daily | %s | %s | %s", symbol, date, r)


class BarTaskExecutor(object):

    TAG = BAR_TAG

    def __init__(self, task, storage):
        assert isinstance(task, Task)
        assert isinstance(storage, KlineStorage)
        self.task = task
        self.storage = storage
    
    def publish(self, symbols=None, dates=None):
        tasks = list(self.task.select(symbols, dates, self.TAG, sort="symbol"))
        for symbol, date, tag in tasks:
            try:
                results = self.handle(symbol, date)
            except Exception as e:
                logging.error("kline load bar | %s | %s | %s", symbol, date, e)
            else:
                self._fill(results)

    def _fill(self, results):
        for s, d, r in results:
            logging.warning("kline load bar | %s | %s | %s", s, d, r)
            self.task.fill(s, d, self.TAG, r)

    def handle(self, symbol, date):
        data = api.bar(symbol, date)
        if len(data.index):
            result = self.storage.write(data)
        else:
            result = [(symbol, date, -1)]
        return result

    def check(self, symbols=None, dates=None, tag=0):
        tasks = list(self.task.select(symbols, dates, self.TAG, tag, "symbol"))
        for symbol, date, _tag in tasks:
            r = self.storage.check(symbol, date)
            if r != _tag:
                self.task.fill(symbol, date, self.TAG, r)
            logging.warning("kline check bar | %s | %s | %s", symbol, date, r)
