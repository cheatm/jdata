from jaqsd import env
from pymongo import MongoClient, UpdateOne
import pandas as pd


def get_client():
    if "CLIENT" in globals():
        return globals()["CLIENT"]
    else:
        globals()["CLIENT"] = MongoClient(env.MONGODB_URI)
        return globals()["CLIENT"]


def get_collection(name):
    db, col = name.split(".", 1)
    return get_client()[db][col]


def make_append(data, index):
    assert isinstance(data, pd.DataFrame)
    for doc in data.to_dict("record"):
        filters = {name: doc[name] for name in index}
        yield UpdateOne(filters, {"$setOnInsert": doc}, True)
