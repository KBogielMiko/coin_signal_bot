import datetime
import json
from kucoin.client import Market
import numpy as np
import requests
import pandas as pd
import sched
import time
import warnings

from kucoin.base_request.base_request import KucoinBaseRestApi

#KuCoin API initialization

client = Market(url='https://api.kucoin.com')
api_key = 'api_key'
api_secret = 'api_secret'
api_passphrase = 'api_passphrase'

#Function which gets the data and process data for further operations
def api_call():
    tickers = client.get_all_tickers()
    tickers = tickers['ticker']
    wrong_keys = ('makerCoefficient','takerCoefficient','makerFeeRate','takerFeeRate','last','symbolName')
    for i in tickers:
        for k in wrong_keys:
            i.pop(k, None)
    df = pd.DataFrame.from_dict(tickers)
    df = df.set_index('symbol')
    return df

#Function which gets the data once a day
def stats_24h():
    return api_call()

#Function which gets the data once an hour
def stats_1h():
    df = api_call()
    df = df[df.index.isin(stats_24h().index)]                                                  # in case of new rows during the day
    return df

#Function which gets the current data and compares it with previous data
def current_stats():

    df_now = api_call()
    df_1h = stats_1h()

    df_now = df_now[df_now.index.isin(stats_1h().index)]                                      # in case of new rows during the day
    df_now.rename(columns = {'symbol':'symbol_','buy':'buy_','sell':'sell_'                   # rename columns of 1df before merge 2 dfs
                            ,'changeRate':'changeRate_','changePrice':'changePrice_'
                            ,'high':'high_','low':'low_','vol':'vol_',
                            'volValue':'volValue_','averagePrice':'averagePrice_'}, inplace = True)

    df = pd.merge(df_1h, df_now, left_index=True, right_index=True)                           # merge 2dfs

    for i in df:                                    # replace all values to floats in df
        df[i] = df[i].astype(float)

    df['hour_change'] = df['buy_'] / df['buy']     # generate new column in df - compare price change

    signal_list = []                               # list of tickers with potential signal
    for i in range(len(df['hour_change'])):        # in case of price increase, row appends to signal_list
        if df['hour_change'][i] > 1:
            d = {df.iloc[i].name: df.iloc[i]}
            signal_list.append(d)

    if len(signal_list) > 0:                      # in case of non-empty list, Twitter activity and Telegram operations
        def telegram_bot():                       # Telegram bot initialization
            bot_token = 'bot_token'
            bot_chat_id = 'bot_chat_id'
            bot_msg = 'bot_msg'
            send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chat_id + \
                        '&parse_mode=MarkdownV2&text=' + bot_msg
            response = requests.get(send_text)

            return response.json()

######## scheduler - last part

schedule.every().day.at("00:00").do(stats_24h)
schedule.every().minute.at(":00").do(stats_1h)
schedule.every().minute.at(":01").do(current_stats)

while True:
    schedule.run_pending()
    time.sleep(30)
