from jaqs.data import DataApi
import jaqsd.env as env
import logging


def login(username, password):
    api = DataApi(env.ADDR)
    api.login(username, password)
    logging.warning("login | %s", username)
    return api


def get_api(username=None, password=None):
    if "API" in globals():
        return globals()["API"]
    else:
        if not username:
            username = env.USERNAME
        if not password:
            password = env.PASSWORD
        globals()["API"] = login(username, password)
        return globals()["API"]


def close():
    api = globals().get("API", None)
    if api is not None:
        api.logout()
        api.close()


def query(*args, **kwargs):
    data, msg = get_api().query(*args, **kwargs)
    if msg == "0,":
        return data
    else:
        raise Exception(msg)


def get_A_stocks():
    data = query("jz.instrumentInfo", "market=SZ,SH&inst_type=1", "symbol")
    return data["symbol"]


def get_A_indexes():
    data = query("jz.instrumentInfo", "market=SZ,SH&inst_type=100&status=1", "symbol")
    return data["symbol"]


def get_trade_days(start=19900101, end=20291231):
    data = query("jz.secTradeCal", "start_date={}&end_date={}".format(start, end))
    return data["trade_date"]


def get_api_params(view):
    data = query("help.apiParam", "api=lb.secFinIndicators")
    return data


def daily(symbols, start, end):
    data, msg = get_api().daily(symbols, start, end)
    if msg == "0,":
        return data
    else:
        raise Exception(msg)


def bar(symbols, trade_date):
    data, msg = get_api().bar(symbols, trade_date=trade_date)
    if msg == "0,":
        return data
    else:
        raise Exception(msg)


def main():
    frame = get_A_stocks()


if __name__ == '__main__':
    main()