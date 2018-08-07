from jaqsd import conf
from pymongo import MongoClient


def get_client():
    if "CLIENT" in globals():
        return globals()["CLIENT"]
    else:
        globals()["CLIENT"] = MongoClient(env.MONGODB_URI)
        return globals()["CLIENT"]


def get_collection(name):
    db, col = name.split(".", 1)
    return get_client()[db][col]
