from jaqsd.kline.mongodb import MongoDBTask
from jaqsd.kline.conf import TASK
from jaqsd.utils.mongodb import get_collection
from jaqsd.kline.framework import TaskIndex
from itertools import product
from datetime import datetime
from jaqsd import api
import pandas as pd



def today():
    date = datetime.today()
    return date.year*10000 + date.month*100 + date.day


def init_from_env():
    return TaskIndex(MongoDBTask(get_collection(TASK)))


def create(symbols=None, start=None, end=None):
    task = init_from_env()
    task.create(symbols, start, end)


if __name__ == '__main__':
    create(["000001.SZ", "600000.SH", "000024.SZ", "000916.SZ"], 20180806)