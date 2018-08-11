from jaqsd.kline.framework import Task, KlineStorage
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from jaqsd.kline.conf import DAILY_TAG, BAR_TAG, STATUS_TAG
from datetime import datetime
import logging


class MongoDBTask(Task):

    SYMBOL = "_s"
    DATE = "_d"
    MODIFY = "_m"

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.collection.create_index([(self.SYMBOL, 1), (self.DATE, 1)], unique=True, background=True)
    
    @classmethod
    def init_from_env(cls):
        from jaqsd.utils.mongodb import get_collection
        from jaqsd.kline.conf import TASK
        return cls(get_collection(TASK))

    def create(self, symbol, date):
        doc = {
            self.SYMBOL: symbol,
            self.DATE: date,
            DAILY_TAG: 0,
            BAR_TAG: 0,
            self.MODIFY: datetime.now()
        }
        try:
            self.collection.insert_one(doc)
        except DuplicateKeyError:
            return 0
        else:
            return 1
        
    def select(self, symbols=None, dates=None, tag=DAILY_TAG, value=0, sort="date"):
        if tag not in [DAILY_TAG, BAR_TAG]:
            raise ValueError("tag not valid: %s" % tag)

        filters = {STATUS_TAG: {"$ne": 0}}
        if value is not None:
            filters[tag] = value

        if isinstance(symbols, list):
            filters[self.SYMBOL] = {"$in": symbols}

        if isinstance(dates, tuple):
            start, end = dates
            if start:
                filters[self.DATE] = {"$gte": start}
            if end:
                filters.setdefault("date", {})["$lte"] = end
        elif isinstance(dates, list):
             filters[self.DATE] = {"$in": dates}
        cursor = self.collection.find(filters)
        if sort == "date":
            cursor.sort(self.DATE)
        elif sort == "symbol":
            cursor.sort(self.SYMBOL)
        docs = list(cursor)
        for doc in docs:
            yield doc[self.SYMBOL], doc[self.DATE], doc[tag]
    
    def fill(self, symbol, date, tag, value):
        filters = {
            self.SYMBOL: symbol,
            self.DATE: date
        }
        doc = {
            tag: value,
            self.MODIFY: datetime.now()
        }

        return self.collection.update_one(
            filters,
            {"$set": doc}
        )
    
    def recent(self):
        doc = self.collection.find_one(sort=[(self.DATE, -1)])
        if doc:
            return doc[self.DATE]
        else:
            return None

    # def check(self, symbol, date, tag):
    #     filters = {
    #         self.SYMBOL: symbol,
    #         self.DATE: date,

    #     }

def fold(code):
    if code.endswith("XSHE"):
        return code[:-4] + "SZ"
    elif code.endswith("XSHG"):
        return code[:-4] + "SH"
    else:
        return code


def defold(code):
    if code.endswith("SZ"):
        return code[:-2] + "XSHE"
    elif code.endswith("SH"):
        return code[:-2] + "XSHG"
    else:
        return code


KLINE_COLUMNS = ["trade_date", "open", "close", "high", "low", "volume", "turnover"]


class MongoDBDaily(KlineStorage):

    def __init__(self, db):
        assert isinstance(db, Database)
        self.db = db

    @classmethod
    def init_from_env(cls):
        from jaqsd.utils.mongodb import get_client
        from jaqsd.kline.conf import DAILY
        return cls(get_client()[DAILY])
    
    @staticmethod
    def date2time(date):
        return datetime.strptime(str(date), "%Y%m%d").replace(hour=15)

    def write(self, data):
        data = data[data["trade_status"]=="交易"]
        results = []
        for symbol, frame in data[KLINE_COLUMNS].groupby(data["symbol"]):
            for result in self.update(symbol, frame):
                results.append(result)
        return results

    def update(self, symbol, data):
        col_name = defold(symbol)
        collection = self.db[col_name]
        for doc in data.to_dict("record"):
            date = int(doc.pop("trade_date"))
            ft = {"datetime": self.date2time(date)}
            doc.update(ft)
            try:
                result = collection.update_one(ft, {"$set": doc}, True)
            except Exception as e:
                logging.error("kline update | %s | %s | %s", symbol, date, e)
            else:
                if result.matched_count or result.upserted_id:
                    yield symbol, date, 1
                else:
                    yield symbol, date, 0
    
    def check(self, symbol, date):
        col_name = defold(symbol)
        time = self.date2time(date)
        return self.db[col_name].find({"datetime": time}).count()


BAR_COLUMNS = ["trade_date", "time", "open", "high", "low", "close", "volume", "turnover"]


def join_dt(date, time):
    y, m, d = split(date)
    H, M, S = split(time)
    return datetime(y, m, d, H, M, S)


def split(num):
    x = num % 100
    y = int(num/100) % 100
    z = int(num/10000)
    return z, y, x  


class MongoDBBar(KlineStorage):

    def __init__(self, db):
        assert isinstance(db, Database)
        self.db = db
    
    @classmethod
    def init_from_env(cls):
        from jaqsd.utils.mongodb import get_client
        from jaqsd.kline.conf import BAR
        return cls(get_client()[BAR])

    @staticmethod
    def date2time(date):
        return datetime.strptime(str(date), "%Y%m%d")

    def check(self, symbol, date):
        col_name = defold(symbol)
        time = self.date2time(date)
        collection = self.db[col_name]
        doc = collection.find_one({"_d": time}, {"_l": 1})
        if doc:
            return doc.get("_l", 0)
        else:
            return 0
    
    def write(self, data):
        results = []
        data = data.set_index("symbol")[BAR_COLUMNS]
        for symbol, _data in data.groupby(level=0):
            for date, bar in _data.set_index("trade_date").groupby(level=0):
                times = bar.pop("time")
                bar["datetime"] = [join_dt(date, time) for time in times]
                r = self._write(symbol, date, bar)
                results.append(r)
        return results
    
    def _write(self, symbol, date, frame):
        col_name = defold(symbol)
        collection = self.db[col_name]
        doc = frame.to_dict("list")
        _d = datetime(*split(date))
        _l = len(frame.index)
        doc["_d"] = _d
        doc["_l"] = _l
        r = collection.update_one({"_d": _d}, {"$set": doc}, upsert=True)
        if r.matched_count or r.upserted_id:
            return symbol, date, _l
        else:
            return symbol, date, 0
