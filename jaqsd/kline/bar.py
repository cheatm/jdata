from jaqsd.kline.framework import BarTaskExecutor
from jaqsd.kline.mongodb import MongoDBTask, MongoDBBar
import click


def init_executor():
    return BarTaskExecutor(MongoDBTask.init_from_env(), MongoDBBar.init_from_env())


@click.command(help="Valid commands: download, check. Default: download.")
@click.argument("commands", nargs=-1)
@click.option("--symbol", "-s", default=None, type=click.STRING)
@click.option("--begin", "-b", default=None, type=click.INT)
@click.option("--end", "-e", default=None, type=click.INT)
@click.option("--cover", "-c", default=False, is_flag=True)
def command(commands, symbol, begin, end, cover):
    if not commands:
        commands = ["download"]
    
    if symbol:
        symbol = symbol.split(",")
    
    dates = None
    if begin or end:
        dates = (begin, end)
    
    executor = init_executor()
    for cmd in commands:
        if cmd == "download":
            executor.publish(symbol, dates)
        elif cmd == "check":
            executor.check(symbol, dates, None if cover else 0)


if __name__ == '__main__':
    command()