import datetime
import io
import json
from kucoin.client import Market
import matplotlib.pyplot as plt
import numpy as np
import requests
import pandas as pd
import re
import sched
import time
import tweepy
import warnings

from kucoin.base_request.base_request import KucoinBaseRestApi


#KuCoin API initialization
kucoin_client = Market(url='https://api.kucoin.com')
kucoin_api_key = 'api_key'
kucoin_api_secret = 'api_secret'
kucoin_api_passphrase = 'api_passphrase'

#Telegram API initialization
telegram_bot_token = 'telegram_bot_token'
telegram_bot_chat_id = -'telegram_bot_chat_id'

#Twitter API initialization
twitter_bearer_token = 'twitter_bearer_token'
twitter_client = tweepy.Client(twitter_bearer_token)


#Function which gets and process current 24h data for further operations
def api_call():
    tickers = kucoin_client.get_all_tickers()
    tickers = tickers['ticker']
    wrong_keys = ('makerCoefficient','takerCoefficient','makerFeeRate','takerFeeRate','last','symbolName')
    for i in tickers:                      # remove all unnecesary keys - function will be called few times (optimilization)
        for k in wrong_keys:
            i.pop(k, None)
    df = pd.DataFrame.from_dict(tickers)
    df = df.set_index('symbol')
    return df

#Function which gets the data once a week for weekly stats
def stats_1W_00():
    return api_call()

#Function which gets the data once a day to compare daily stats
def stats_1D_00():
    return api_call()

#Function which gets the data once a day for intraday stats
def stats_24h_00():
    return api_call()

#Function which gets the data once an hour for intraday stats
def stats_1h_00():
    df = api_call()
    df = df[df.index.isin(stats_24h().index)]                                                  # in case of new rows during the day
    return df

#Function which gets the current data and compares it with previous data
def current_stats():

    df_now = api_call()
    df_1h = stats_1h_00()

    df_now = df_now[df_now.index.isin(stats_1h().index)]                                      # in case of new rows during the day
    df_now.rename(columns = {'symbol':'symbol_','buy':'buy_','sell':'sell_'                   # rename columns of 1df before merge 2 dfs
                            ,'changeRate':'changeRate_','changePrice':'changePrice_'
                            ,'high':'high_','low':'low_','vol':'vol_',
                            'volValue':'volValue_','averagePrice':'averagePrice_'}, inplace = True)

    df = pd.merge(df_1h, df_now, left_index=True, right_index=True)                           # merge 2dfs

    df['hour_change'] = df['buy_'] / df['buy']     # generate new column in df - compare price change

    signal_list = []                               # list of tickers with potential signal
    for i in range(len(df['hour_change'])):        # in case of price increase, row appends to signal_list
        if df['hour_change'][i] > 1.1:
            signal_list.append(df.index[i])

    if len(signal_list) > 0:                # in case of non-empty list, Twitter activity and Telegram operations
        for i in signal_list:

            escaped_i = re.escape(i)
            signal_df = api_call()

            midnight_price = float(stats_24h_00().loc[i]['buy'])
            current_price = float(signal_df.loc[i]['buy'])
            price_change = round(((current_price/midnight_price)-1)*100, 2)

            midnight_volume = float(stats_24h_00().loc[i]['volValue'])
            current_volume = float(signal_df.loc[i]['volValue'])
            volume_change = round(((current_volume/midnight_volume)-1)*100, 2)

            price_1h_ago = float(stats_1h_00().loc[i]['buy'])
            price_change_1h = round(((current_price/price_1h_ago)-1)*100,2)

            btc_midnight_price = float(stats_24h_00().loc['BTC-USDT']['buy'])
            btc_current_price = float(signal_df.loc['BTC-USDT']['buy'])
            btc_price_change = round(((btc_current_price/btc_midnight_price)-1)*100, 2)

            btc_midnight_volume = float(stats_24h_00().loc['BTC-USDT']['volValue'])
            btc_current_volume = float(signal_df.loc['BTC-USDT']['volValue'])
            btc_volume_change = round(((btc_current_volume/btc_midnight_volume)-1)*100, 2)

            eth_midnight_price = float(stats_24h_00().loc['ETH-USDT']['buy'])
            eth_current_price = float(signal_df.loc['ETH-USDT']['buy'])
            eth_price_change = round(((eth_current_price/eth_midnight_price)-1)*100, 2)

            eth_midnight_volume = float(stats_24h_00().loc['ETH-USDT']['volValue'])
            eth_current_volume = float(signal_df.loc['ETH-USDT']['volValue'])
            eth_volume_change = round(((eth_current_volume/eth_midnight_volume)-1)*100, 2)

            bot_message = ('bot message')
            send_text = 'https://api.telegram.org/bot' + telegram_bot_token + '/sendMessage?chat_id=' + telegram_bot_chat_id + \
                '&parse_mode=MarkdownV2&text=' + bot_message
            response = requests.get(send_text)
            return response.json()
        telegram_bot_msg()

    for k in signal_list:             # Twitter activity plots

        def telegram_tt_plots(j):

            response = twitter_client.get_recent_tweets_count(j, granularity="hour")

            df = pd.DataFrame(data=response.data)
            df = df.iloc[-24:-1, :]

            df['end'] = pd.to_datetime(df['end'])
            df['hour'] = df['end'].dt.hour

            df_plot = df.loc[:, ['tweet_count','hour']]
            df_plot['hour'] = df_plot['hour'].astype(str)

            plt.bar(df_plot['hour'], df_plot['tweet_count'])
            plt.xlabel('Hour')
            plt.ylabel('Tweet Count')
            plt.title('#{}: Twitter activity graph - 1 hour'.format(j))
            plt.xticks(rotation=90)
            file_name = f"{j}__{i}_plot.png"
            plt.savefig(file_name)
            plt.close()

            image = open(file_name, "rb")
            data = {"photo": image}
            url = "https://api.telegram.org/bot" + telegram_bot_token +"/sendPhoto"
            files = {'photo': (file_name, open(file_name, 'rb'), 'image/png')}
            data = {'chat_id': 'telegram_bot_chat_id'}

            response = requests.post(url, files=files, data=data)

            return response

        for i in signal_list:                          # 2 plots - for example for 'BTC-USDT' - #BTC & #BTCUSDT
            hashtag_1 = i[:i.index("-")]
            hashtag_2 = i.replace("-", "")
            hashtag_list = [hashtag_1, hashtag_2]
            for j in hashtag_list:
                    telegram_tt_plots(j)

def weekly_stats():

    df_weekly_now = api_call()
    df_weekly = stats_1W_00()

    df_weekly_now = df_weekly_now[df_weekly_now.index.isin(df_weekly.index)]                                      # in case of new rows during the day
    df_weekly_now.rename(columns = {'symbol':'symbol_','buy':'buy_','sell':'sell_'                                 # rename columns of 1df before merge 2 dfs
                            ,'changeRate':'changeRate_','changePrice':'changePrice_'
                            ,'high':'high_','low':'low_','vol':'vol_',
                            'volValue':'volValue_','averagePrice':'averagePrice_'}, inplace = True)

    df = pd.merge(df_weekly, df_weekly_now, left_index=True, right_index=True)                           # merge 2dfs

    df['weekly_change'] = df['buy_'] / df['buy']     # generate new column in df - compare price change

    signal_list = []                               # list of tickers with potential signal
    for i in range(len(df['weekly_change'])):        # in case of price increase, row appends to signal_list
        if df['weekly_change'][i] > 2:
            signal_list.append(df.index[i])


    if len(signal_list) > 0:                # in case of non-empty list, Twitter activity and Telegram operations
        for i in signal_list:
            escaped_i = re.escape(i)

            current_price = float(df_weekly_now.loc[i]['buy'])
            price_week_ago = float(stats_1W_00().loc[i]['buy'])
            price_change = round(((current_price/price_week_ago)-1)*100, 2)

            bot_message = ('bot message')

            send_text = 'https://api.telegram.org/bot' + telegram_bot_token + '/sendMessage?chat_id=' + telegram_bot_chat_id + \
                '&parse_mode=MarkdownV2&text=' + bot_message
            response = requests.get(send_text)
            return response.json()
        telegram_bot_msg()


def daily_stats():

    df_daily_now = api_call()
    df_daily = stats_1D_00()

    df_daily_now = df_weekly_now[df_weekly_now.index.isin(df_weekly.index)]                                      # in case of new rows during the day
    df_daily_now.rename(columns = {'symbol':'symbol_','buy':'buy_','sell':'sell_'                                 # rename columns of 1df before merge 2 dfs
                            ,'changeRate':'changeRate_','changePrice':'changePrice_'
                            ,'high':'high_','low':'low_','vol':'vol_',
                            'volValue':'volValue_','averagePrice':'averagePrice_'}, inplace = True)

    df = pd.merge(df_daily, df_daily_now, left_index=True, right_index=True)                           # merge 2dfs

    df['daily_change'] = df['buy_'] / df['buy']     # generate new column in df - compare price change

    signal_list = []                               # list of tickers with potential signal
    for i in range(len(df['daily_change'])):        # in case of price increase, row appends to signal_list
        if df['daily_change'][i] > 1.5:
            signal_list.append(df.index[i])


    if len(signal_list) > 0:                # in case of non-empty list, Twitter activity and Telegram operations
        for i in signal_list:
            escaped_i = re.escape(i)

            current_price = float(df_daily_now.loc[i]['buy'])
            price_day_ago = float(stats_1D_00().loc[i]['buy'])
            price_change = round(((current_price/price_day_ago)-1)*100, 2)

            bot_message = ('bot message')

            send_text = 'https://api.telegram.org/bot' + telegram_bot_token + '/sendMessage?chat_id=' + telegram_bot_chat_id + \
                '&parse_mode=MarkdownV2&text=' + bot_message
            response = requests.get(send_text)
            return response.json()
        telegram_bot_msg()


######## scheduler - last part

schedule.every().monday.at("00:00").do(stats_1W_00)
schedule.every().sunday.at("23:59").do(weekly_stats)

schedule.every().day.at("00:00").do(stats_1D_00)
schedule.every().day.at("23:59").do(daily_stats)

schedule.every().minute.at(":00").do(stats_1h_00)
schedule.every().minute.at(":59").do(current_stats)

while True:
    schedule.run_pending()
    time.sleep(30)
