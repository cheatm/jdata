

class TaskTable(object):

    def create(self, view, tag, params):
        pass
    
    def select(self, views=None, tag=None):
        pass
    
    def fill(self, view, tag):
        pass
    
    def clear(self, view, tag):
        pass


from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
from jaqsd import api
import logging


# class MongoDBTask(object):

#     VIEW = "view"
#     TAG = "tag"
#     PARAMS = "params"
#     EXPIRE = "expire"
#     LENGTH = "_l"

#     def __init__(self, collection):
#         assert isinstance(collection, Collection)
#         self.collection = collection
#         self.collection.create_index([(VIEW, 1), (TAG, 1)], background=True, unique=True)
    
#     def create(self, view, tag, params):
#         doc = {
#             self.VIEW: view,
#             self.TAG: tag,
#             self.PARAMS: params,
#             self.LENGTH: 0,
#         }

#         try:
#             self.collection.insert_one(doc)
#         except DuplicateKeyError:
#             pass
    
#     def select(self, views=None, tag=None):
#         filters = {self.LENGTH: 0}

def create(collection, view, params, tag):
    doc = {"view": view, "params": params, "tag": tag}
    try:
        collection.insert_one(doc)
    except DuplicateKeyError:
        pass
    else:
        logging.warning("create | %s | %s", view, tag)


def get_fields(view):
    data = api.get_api_params(view)
    # print(data)
    return ",".join(data["param"][data["ptype"] == "OUT"])


client = MongoClient("192.168.0.104,192.168.0.105")
task = client["log"]["weekly_finance"]
task.create_index([("view", 1), ("tag", 1)], unique=True, background=True)

stocks = list(api.get_A_stocks())

parts = []
for i in range(0, len(stocks), 100):
    parts.append(",".join(stocks[i:i+100]))

from jaqsd.structure import Income, CashFlow, BalanceSheet, FinIndicator, SecDividend


# for view in ["lb.income", "lb.cashFlow", "lb.balanceSheet", "lb.secFinIndicators", "lb.secDividend"]:
# for view in [Income, CashFlow, BalanceSheet, FinIndicator, SecDividend]:
#     # fields = get_fields(view)
#     tag = 0
#     for symbols in parts:
#         # params = {
#         #     "view": view,
#         #     "filter": "symbol=%s&start_date=19900101&end_date=20291231" % symbols,
#         #     "fields": fields
#         # }
#         params = view(symbol=symbols, start_date="19900101", end_date="20291231")
#         create(task, view.view, params, tag=tag)
#         tag += 1


def read(collection):
    docs = list(collection.find({"_l": 0}))
    for doc in docs:
        yield doc["view"], doc["tag"], doc["params"]


def fill(collection, view, tag, length):
    collection.update_one({"view": view, "tag": tag}, {"$set": {"_l": length}})


def query(task_, view, tag, params):
    db, col = view.split(".")
    col = col + "_temp"
    collection = client[db][col]
    try:
        data = api.query(**params)
        l = len(data.index)
        if l:
            insert(collection, data)
            fill(task_, view, tag, l)
        else:
            l = -1
    except Exception as e:
        logging.error("query | %s | %s | %s | %s", view, tag, params, e)
    else:
        logging.warning("query | %s | %s | %s | %s", view, tag, params, l)


def insert(collection, data):
    collection.insert_many(data.to_dict("record"))


def run():
    
    docs = list(read(task))
    if len(docs) == 0:
        return
    print(len(docs))
    for v, t, p in docs:
        query(task, v, t, p)
    run()


run()