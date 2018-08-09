from jaqsd.kline.framework import Task
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from jaqsd.kline.conf import DAILY_TAG, BAR_TAG


class MongoDBTask(Task):

    SYMBOL = "_s"
    DATE = "_d"
    STATUS = "_t"

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.collection.create_index([(self.SYMBOL, 1), (self.DATE, 1)], unique=True, background=True)
    
    def create(self, symbol, date):
        doc = {
            self.SYMBOL: symbol,
            self.DATE: date,
            DAILY_TAG: 0,
            BAR_TAG: 0
        }
        try:
            self.collection.insert_one(doc)
        except DuplicateKeyError:
            return 0
        else:
            return 1
        
    def select(self, symbols=None, dates=None, tag=DAILY_TAG):
        if tag not in [DAILY_TAG, BAR_TAG]:
            raise ValueError("tag not valid: %s" % tag)

        filters = {tag: 0, self.STATUS: {"$ne": 0}}

        if isinstance(symbols, list):
            filters["symbol"] = {"$in": symbols}

        if isinstance(dates, tuple):
            start, end = dates
            if start:
                filters["date"] = {"$gte": start}
            if end:
                filters.setdefault("date", {})["$lte"] = end
        
        docs = list(self.collection.find(filters))
        for doc in docs:
            yield doc[self.SYMBOL], doc[self.DATE], doc[tag]
    
    def fill(self, symbol, date, tag, value):
        filters = {
            self.SYMBOL: symbol,
            self.DATE: date
        }

        return self.collection.update_one(
            filters,
            {"$set": {tag: value}}
        )
    
    # def check(self, symbol, date, tag):
    #     filters = {
    #         self.SYMBOL: symbol,
    #         self.DATE: date,

    #     }
