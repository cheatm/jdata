import os


DAILY = os.environ.get("KLINE_DAILY", "Stock_D")
BAR = os.environ.get("KLINE_BAR", "Stock_1M")
TASK = os.environ.get("KLINE_TASK", "log.kline_task")

DAILY_TAG = "D"
BAR_TAG = "1M"
STATUS_TAG = "T"
