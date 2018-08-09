from datetime import datetime, timedelta
from jaqsd import env
from jaqsd.utils.mongodb import get_client, get_collection
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
from jaqsd import api
from jaqsd.utils import logger
import logging
from jaqsd.structure import Income, CashFlow, BalanceSheet, FinIndicator, SecDividend, ProfitExpress
import os
from jaqsd.finance.conf import WEEKLY, LB, VIEWS, TABLES, INDEXES


class TaskTable(object):

    # Create an index record.
    def create(self, view, tag, params):
        pass
    
    # Query unfilled index records.
    def select(self, views=None, tag=None):
        pass
    
    # Fill an index record when the task is done.
    def fill(self, view, tag, length, expire=None):
        pass
    
    # Reset an filled index record to unfilled
    def clear(self, view, tag, expire=None):
        pass
    
    # Check if all index records of a specified view are filled. True if all filled else False. 
    def check(self, view):
        return False


class TableWriter(object):

    # Insert data (DataFrame) into specified view (Table)
    def insert(self, view, data):
        pass
    
    # Replace old table with nearly created table
    def replace(self, view):
        pass
    

class FrameWork(object):
    
    def __init__(self, task, writer):
        assert isinstance(task, TaskTable)
        assert isinstance(writer, TableWriter)
        self.task = task
        self.writer = writer
    
    # Create index
    def create(self):
        parts = get_stock_parts()
        for view, structure in VIEWS.items():
            count = 0 
            for symbols in parts:
                params = structure(symbol=symbols)
                self.task.create(view, count, params)
                count += 1

    # Fulfill tasks (download data and store) by index
    def publish(self):
        for view, tag, expire, params in self.task.select():
            self.handle(view, tag, expire, params)
    
    # Replace old tables with nearly created tables
    def replace(self):
        for view in VIEWS.keys():
            self._replace(view)
    
    def _replace(self, view):
        if self.task.check(view):
            self.writer.replace(view)
            logging.warning("replace view | %s | ok", view)
        else:
            logging.error("replace view | %s | not all filled", view)

    # Fulfill single task
    def handle(self, view, tag, expire, params):
        try:
            # Query data by params in task
            data = api.query(**params)
            if len(data.index) > 0:
                # If data is not empty, store data and set result to length of data
                result = self.writer.insert(view, data)
            else:
                # If data is empty, set result to -1 which means data of this task is unreachable.
                result = -1
            # Fill index with result.
            self.task.fill(view, tag, result, expire)
        except Exception as e:
            logging.error("download finance weekly | %s | %s | %s", view, tag, e)
        else:
            logging.warning("download finance weekly | %s | %s | %s", view, tag, result)


# Get weekend date of this week. Int, YYYYMMDD
def weekend():
    date = datetime.today()
    date = date + timedelta(days=7-date.isoweekday())
    return date.year*10000 + date.month*100 + date.day


def today():
    date = datetime.today()
    return date.year*10000 + date.month*100 + date.day


# Split list of stock symbols into smaller parts
def get_stock_parts():
    stocks = list(api.get_A_stocks())

    parts = []
    for i in range(0, len(stocks), 100):
        parts.append(",".join(stocks[i:i+100]))
    return parts


class MongoDBTask(TaskTable):

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
    
    def check(self, view):
        filters = {"expire": weekend(), "view": view}
        cursor = self.collection.find(filters)
        for doc in list(cursor):
            if doc["_l"] == 0:
                return False
        return True

    def fill(self, view, tag, length, expire=weekend()):
        filters = {
            "view": view,
            "tag": tag,
            "expire": expire
        }
        update = {"$set": {"_l": length, "finish": datetime.now()}}
        result = self.collection.update_one(filters, update)
        return result.modified_count
    
    def clear(self, view, tag, expire=weekend()):
        return self.fill(view, tag, 0, expire)


class MongoDBWriter(TableWriter):

    def __init__(self, db):
        self.db = db
    
    def get_name(self, view):
        return TABLES[view]
    
    def get_temp_name(self, view):
        return self.get_name(view) + "_temp"
    
    def get_index_conf(self, view):
        return INDEXES[view]

    def insert(self, view, data):
        collection = self.db[self.get_temp_name(view)]
        result = self._insert(collection, data)
        return result

    @staticmethod
    def _insert(collection, data):
        return collection.insert_many(data.to_dict("record")).inserted_ids.__len__()

    @staticmethod
    def _exists(collection):
        return collection.name in collection.database.collection_names()

    def replace(self, view):
        collection =self.db[self.get_temp_name(view)]
        if not self._exists(collection):
            logging.debug("replace | %s | not exists", collection.name)
            return
        index_names = self.get_index_conf(view)
        for name in index_names:
            collection.create_index(name, background=True)
            logging.debug("create index | %s | %s", view, name)
        origin = self.db[self.get_name(view)]
        name = origin.name
        collection.rename(name, dropTarget=True)

def init_from_env():
    client = get_client()
    task_col = get_collection(WEEKLY)
    lb = client[LB]
    task = MongoDBTask(task_col)
    writer = MongoDBWriter(lb)
    return FrameWork(task, writer)


import click


@click.command(help="Valid commands: create, publish, replace.")
@click.argument("commands", nargs=-1)
def operate(commands):
    fw = init_from_env()
    for cmd in commands:
        if cmd in {"create", "publish", "replace"}:
            logging.warning("finance weekly | Execute command | %s", cmd)
            getattr(fw, cmd)()
        else:
            logging.error("finance weekly | No such command | %s", cmd)


if __name__ == '__main__':
    operate()