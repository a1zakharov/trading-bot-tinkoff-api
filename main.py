from tinkoff.invest import Client, RequestError, CandleInterval, HistoricCandle, OrderDirection, OrderType
import schedule
import my_token
from datetime import datetime, timedelta
import numpy as np
from ta.trend import ema_indicator
from pandas import DataFrame
import pandas as pd
import time

def cast_money(v):
    return v.units + v.nano / 1e9 # nano - 9 нулей

def create_df(candles : [HistoricCandle]): # type: ignore
    df = DataFrame([{
        'time': c.time,
        'volume': c.volume,
        'open': cast_money(c.open),
        'close': cast_money(c.close),
        'high': cast_money(c.high),
        'low': cast_money(c.low),
    } for c in candles])
    return df

def date():
    global res_for_bot
    global res_for_bot2
    try:
        with Client(my_token.token) as client:
            r = client.market_data.get_candles(
                figi=my_token.figi_tmos,
                from_=datetime.now() - timedelta(days=365),
                to=datetime.now(),
                interval=CandleInterval.CANDLE_INTERVAL_DAY # см. utils.get_all_candles
            )

            df = create_df(r.candles)
            df['ema60'] = ema_indicator(close=df['open'], window=60)
            df['ema9'] = ema_indicator(close=df['open'], window=9)

            close_ema9 = df[['time', 'close', 'open', 'ema9']].tail(1)
            print(close_ema9)
            close_ema60 = df[['time', 'close', 'open', 'ema60']].tail(1)
            print(close_ema60)

            comparison = np.where((close_ema9['open'] > close_ema9['ema9']))
            print(comparison)
            res_comparison = comparison[0]
            match res_comparison:
                case 0:
                    res_for_bot2 = True
                case _:
                    res_for_bot2 = False

            comparison = np.where((close_ema60['open'] > close_ema60['ema60']))
            print(comparison)
            res_comparison = comparison[0]
            match res_comparison:
                case 0:
                    res_for_bot = True
                case _:
                    res_for_bot = False
            print(res_for_bot)
            print(res_for_bot2)

            re = (df[['time', 'open']]).tail(1)
            print(re)

    except RequestError as e:
        print(str(e))

def buy():
    if res_for_bot2 == True and res_for_bot == True:
        try:
            with Client(my_token.token2) as client:
                r = client.orders.post_order(
                    order_id=str(datetime.now().timestamp()),
                    figi=my_token.figi_tmos,
                    quantity=900,
                    account_id=my_token.account_id_iis,
                    direction=OrderDirection.ORDER_DIRECTION_BUY,
                    order_type=OrderType.ORDER_TYPE_MARKET
                )
                global buy_been_finalized
                buy_been_finalized = True
        except RequestError as e:
            print(str(e))

def sell():
    if buy_been_finalized == True:
        try:
            with Client(my_token.token2) as client:
                r = client.orders.post_order(
                    order_id=str(datetime.now().timestamp()),
                    figi=my_token.figi_tmos,
                    quantity=900,
                    account_id=my_token.account_id_iis,
                    direction=OrderDirection.ORDER_DIRECTION_SELL,
                    order_type=OrderType.ORDER_TYPE_MARKET
                )
        except RequestError as e:
            print(str(e))

schedule.every().day.at("12:00").do(date)
schedule.every().day.at("12:01").do(buy)
schedule.every().day.at("20:30").do(sell)

while True:
         schedule.run_pending()
         time.sleep(5)
