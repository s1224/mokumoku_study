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
conf.read('/work/setting/setting.ini')

##postgres setting
user = conf['postgres']['user']
pw = conf['postgres']['password']
host = conf['postgres']['host']
port = conf['postgres']['port']
database = conf['postgres']['database']

import sys
sys.path.append("/work/def/Kuucoin")
#from Kuucoin import get_query ,contract_active, kuucoin_features_orders, kuucoin_spot_orders, kuucoin_ticker
from Kuucoin import get_query ,contract_active_entry, kuucoin_feature_ticker, kuucoin_spot_ticker, kuucoin_fund_fee_current

def kuucoin_entry():
  for kuucoin_entry in count():
    connection = pg.connect(
                                user = user,
                                password = pw,
                                host =  host,
                                port =  port,
                                database = database
    )
    cursor = connection.cursor()
    df_base = pd.read_sql_query(get_query(filename="kuucoin_fund_fee_check.sql"),connection)
    df_base_check = pd.read_sql_query(get_query(filename="kuucoin_entry_check_rev.sql"),connection)
    now_time = int(math.floor(time.time() * 1000 * 10 ** 0) /(10 ** 0))

    if len(df_base) == 0:
      print('No data')
    elif len(df_base_check) == 0:
      print('Entry duplication')
    elif len(df_base) > 0 and ((now_time - int(df_base.iloc[0]['timepoint'])) < 60000000000000000):
      print('Entry')
      for i in range(len(df_base)):
        #bid = sell, ask = buy
        #feature
        bet_price = kuucoin_feature_ticker(df_base.iloc[i]['symbolname'])
        bid_price_feature = bet_price[0].astype(float)
        ask_price_feature = bet_price[1].astype(float)
        lot_size_feature = contract_active_entry(df_base.iloc[i]['symbolname'])
        lot_size_feature = lot_size_feature.iloc[0]['lotSize']

        ##spot
        spot_symbol = df_base.iloc[0]['spot_symbol']
        bet_price_spot = kuucoin_spot_ticker(spot_symbol)
        bid_price_spot = bet_price_spot[0].astype(float)
        ask_price_spot = bet_price_spot[1].astype(float)

        connection = pg.connect(
                                  user = user,
                                  password = pw,
                                  host =  host,
                                  port =  port,
                                  database = database
        )
        cursor = connection.cursor()

        #feature
        postgres_insert_query = """
                                    insert into
                                        bybit_entry(
                                                    symbol
                                                    ,nextfundingtime
                                                    ,fundingrate
                                                    ,exit_flag
                                                    ,exchangename
                                                    ,featureorspot
                                        )
                                    values(
                                          %s,
                                          %s,
                                          %s,
                                          %s,
                                          %s,
                                          %s
                                     )
                                 """

        
        symbol = str(df_base.iloc[i]['symbolname'])
        nextfundingtime = (kuucoin_fund_fee_current(df_base.iloc[i]['symbolname'])[1]).iloc[0].astype(str)
        fundingrate = (kuucoin_fund_fee_current(df_base.iloc[i]['symbolname'])[0]).iloc[0].astype(float)
        exit_flag = '0'
        exchangename = 'kucoin'
        featureorspot = 'feature'

        cursor.execute(postgres_insert_query,(
                                               symbol
                                               ,nextfundingtime
                                               ,fundingrate
                                               ,exit_flag
                                               ,exchangename
                                               ,featureorspot            
        ))  
        connection.commit()

        # #spot
        postgres_insert_query = """
                                    insert into
                                        bybit_entry(
                                                    nextfundingtime
                                                    ,exit_flag
                                                    ,exchangename
                                                    ,symbol_spot
                                                    ,featureorspot
                                        )
                                    values(
                                          %s,
                                          %s,
                                          %s,
                                          %s,
                                          %s
                                    )
                                """

        nextfundingtime = (kuucoin_fund_fee_current(df_base.iloc[i]['symbolname'])[1]).iloc[0].astype(str)
        exit_flag = '0'
        exchangename = 'kucoin'
        symbol_spot = str(df_base.iloc[i]['spot_symbol'])
        featureorspot = 'spot'
        cursor.execute(postgres_insert_query,(
                                               nextfundingtime
                                               ,exit_flag
                                               ,exchangename
                                               ,symbol_spot
                                               ,featureorspot            
        ))  
        connection.commit()

        time.sleep(1)
        print(str(df_base.iloc[i]['spot_symbol']) + 'done')
      connection = None
      time.sleep(3)
      print('aaa')
    else:
      print('other')
      time.sleep(3)
