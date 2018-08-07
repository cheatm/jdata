from datetime import datetime, timedelta
from jaqsd import env
from jaqsd.utils.mongodb import get_client, get_collection


class TaskTable(object):

    def create(self, view, tag, params):
        pass
    
    def select(self, views=None, tag=None):
        pass
    
    def fill(self, view, tag, length, expire=None):
        pass
    
    def clear(self, view, tag, expire=None):
        pass


def weekend():
    date = datetime.today()
    date = date + timedelta(days=7-date.isoweekday())
    return date.year*10000 + date.month*100 + date.day


def today():
    date = datetime.today()
    return date.year*10000 + date.month*100 + date.day


from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
from jaqsd import api
import logging


class MongoDBTask(object):

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.collection.create_index(
            [("view", 1), ("tag", 1), ("expire", 1)],
            unique=True, background=True
        )
    
    def create(self, view, tag, params):
        expire = weekend()
        doc = {
            "view": view,
            "tag": tag,
            "params": params,
            "expire": expire,
            "_l": 0
        }
        try:
            self.collection.insert_one(doc)
        except DuplicateKeyError:
            pass
        else:
            logging.warning("create | %s | %s | %s", view, tag, expire)

    def select(self, views=None, tag=None):
        filters = {"expire": weekend(), "_l": 0}
        
        if views:
            filters["view"] = {"$in": views}
        if tag:
            filters["tag"] = {"$in": tag}

        cursor = self.collection.find(filters)
        for doc in list(cursor):
            yield doc["view"], doc["tag"], doc["expire"], doc["params"]
    
    def fill(self, view, tag, length, expire=weekend()):
        filters = {
            "view": view,
            "tag": tag,
            "expire": expire
        }
        update = {"$set": {"_l": length}}
        result = self.collection.update_one(filters, update)
        return result.modified_count
    
    def clear(self, view, tag, expire=weekend()):
        return self.fill(view, tag, 0, expire)


def get_stock_parts():
    stocks = list(api.get_A_stocks())

    parts = []
    for i in range(0, len(stocks), 100):
        parts.append(",".join(stocks[i:i+100]))
    return parts


from jaqsd.structure import Income, CashFlow, BalanceSheet, FinIndicator, SecDividend


class FrameWork(object):

    def __init__(self, task):
        assert isinstance(task, TaskTable)
        self.task = task
    
   