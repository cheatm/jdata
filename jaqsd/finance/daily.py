from jaqsd.finance.conf import VIEWS, INDEXES, TABLES, LB, DAILY
from datetime import datetime
from jaqsd import api
from jaqsd import env
from jaqsd.utils.logger import logging
from jaqsd.utils.mongodb import make_append, get_client, get_collection
from pymongo.database import Database, Collection
from pymongo.errors import DuplicateKeyError
import pandas as pd


def get_stocks():
    return ",".join(api.get_A_stocks())


def today():
    date = datetime.today()
    return date.year*10000 + date.month*100 + date.day


class TaskTable(object):

    def create(self, view, date, params):
        pass
    
    def select(self, *views):
        pass
    
    def fill(self, view, date, count, append):
        pass
    
    def check(self, view, date):
        pass


class TableWriter(object):

    def write(self, view, data):
        pass


class FrameWorker(object):

    def __init__(self, task, writer):
        assert isinstance(task, TaskTable)
        assert isinstance(writer, TableWriter)
        self.task = task
        self.writer = writer
    
    def create(self, date=None):
        symbols = get_stocks()
        if not isinstance(date, int):
            date = today()
        for view, structure in VIEWS.items():
            params = structure(symbol=symbols, start_date=date)
            try:
                r = self.task.create(view, date, params)
            except Exception as e:
                logging.error("finance daily | create | %s | %s | %s", view, date, e)
            else:
                if r is not None:
                    logging.warning("finance daily | create | %s | %s | OK", view, date)
                else:
                    logging.debug("finance daily | create | %s | %s | EXISTS", view, date)

    def publish(self):
        for view, date, params in self.task.select():
            self.handle(view, date, params)

    def handle(self, view, date, params):
        try:
            data = api.query(**params)
            count = len(data.index)
            if count > 0:
                result = self.writer.write(view, data)
            else:
                result = 0
            self.task.fill(view, date, count, result)
        except Exception as e:
            logging.error("download finance daily | %s | %s | %s", view, date, e)
        else:
            logging.warning("download finance daily | %s | %s | %s | %s", view, date, count, result)


class MongoDBTask(TaskTable):

    VIEW = "view"
    DATE = "date"
    COUNT = "count"
    APPEND = "append"
    PARAMS = "params"
    MODIFY = "_m"
    CREATE = "_c"


    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.collection.create_index(self.COUNT, background=True)
        self.collection.create_index([(self.VIEW, 1), (self.DATE, 1)], unique=True, background=True)
    
    def create(self, view, date, params):
        now = datetime.now()
        filters = {
            self.VIEW: view,
            self.COUNT: 0
        }
        doc = {
            self.VIEW: view,
            self.DATE: date,
            self.COUNT: 0,
            self.APPEND: 0,
            self.PARAMS: params,
            self.CREATE: now,
            self.MODIFY: now
        }
        try:
            return self.collection.update_one(filters, {"$setOnInsert": doc}, True).upserted_id
        except DuplicateKeyError:
            return None
            
    
    def select(self, *views):
        filters = {
            self.COUNT: 0
        }

        if views:
            filters[self.VIEW] = {"$in": views}
        
        docs = list(self.collection.find(filters))
        for doc in docs:
            yield doc[self.VIEW], doc[self.DATE], doc[self.PARAMS]

    def fill(self, view, date, count, append):
        filters = {
            self.VIEW: view,
            self.DATE: date
        }

        doc = {
            self.COUNT: count,
            self.APPEND: append,
            self.MODIFY: datetime.now()
        }

        return self.collection.update_one(filters, {"$set": doc}).modified_count
    
    def check(self, view, date):
        return self.collection.find_one({self.VIEW: view, self.DATE: date})[self.COUNT]


class MongoDBWriter(TableWriter):

    def __init__(self, db):
        assert isinstance(db, Database)
        self.db = db
    
    def write(self, view, data):
        collection = self.get_collection(view)
        index = INDEXES[view]
        updates = list(make_append(data, index))
        return collection.bulk_write(updates).upserted_count

    def get_collection(self, view):
        return self.db[TABLES[view]]
    

def init_from_env():
    client = get_client()
    task = MongoDBTask(get_collection(DAILY))
    writer = MongoDBWriter(client[LB])
    return FrameWorker(task, writer)


import click


@click.command(help="Valid commands: create, publish")
@click.argument("commands", nargs=-1)
def operate(commands):
    fw = init_from_env()
    for cmd in commands:
        if cmd in {"create", "publish"}:
            logging.warning("finance daily | Execute command | %s", cmd)
            getattr(fw, cmd)()
        else:
            logging.error("finance daily | No such command | %s", cmd)


if __name__ == '__main__':
    operate()