import ccxt as ccxt
import configparser
import pandas as pd
import psycopg2 as pg
from pandas.io.json import json_normalize
from itertools import count, cycle, repeat
import time
import os
import math
import numpy as np

conf = configparser.ConfigParser()
conf.read('./setting/setting.ini')
# api_key = conf['ByBit']['api_key']
# api_secret = conf['ByBit']['secret_key']
api_key_spot = conf['kucoin_spot']['api_key']
api_secret_spot = conf['kucoin_spot']['secret_key']

api_key_feature = conf['kucoin_feature']['api_key']
api_secret_feature = conf['kucoin_feature']['secret_key']
api_password_feature = conf['kucoin_feature']['password']

user = conf['postgres']['user']
pw = conf['postgres']['password']
host = conf['postgres']['host']
port = conf['postgres']['port']
database = conf['postgres']['database']

kucoin_spot = ccxt.kucoin({"apiKey":api_key_spot,"secret":api_secret_spot})
kucoin_feature = ccxt.kucoin({"apiKey":api_key_feature,"secret":api_secret_feature,"password":api_password_feature})

import sys
sys.path.append("./def/Kuucoin")
#from Bybit import order_create, get_query, wallet_balance, coin_base_info, coin_base_ticker
from Kuucoin import get_query ,contract_active, kuucoin_fund_fee_current

def kuucoin_featre_fee():
  for feature_fee in count():
    connection = pg.connect(
                                user = user,
                                password = pw,
                                host =  host,
                                port =  port,
                                database = database
    )
    cursor = connection.cursor()
    df_base = pd.read_sql_query(get_query(filename="kuucoin_fund_fee.sql"),connection)
    connection = None

    now_time = (int(math.floor(time.time() * 1000 * 10 ** 0) /(10 ** 0)))
    now_time_ = float((kuucoin_fund_fee_current('XBTUSDTM')).iloc[0]['timePoint'])
    now_time = (now_time - now_time_)

    if (now_time < 6000000000) and (len(df_base) == 0):
      print('Fund fee check')
      symbols = contract_active()
      symbols = symbols['symbol']
      for i in range(len(symbols)-1):
        fund_fee = kuucoin_fund_fee_current(symbols[i])
        print(fund_fee)
        time.sleep(3)
