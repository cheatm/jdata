from jaqsd.kline.mongodb import MongoDBTask
from jaqsd.kline.conf import TASK
from jaqsd.utils.mongodb import get_collection
from jaqsd.kline.framework import TaskIndex
import click


def init_from_env():
    return TaskIndex(MongoDBTask(get_collection(TASK)))


@click.command()
@click.option("--symbol", "-s", default=None, type=click.STRING)
@click.option("--begin", "-b", default=None, type=click.INT)
@click.option("--end", "-e", default=None, type=click.INT)
def create(symbol=None, begin=None, end=None):
    if symbol:
        symbol = symbol.split(",")

    task = init_from_env()
    task.create(symbol, begin, end)


if __name__ == '__main__':
    create()