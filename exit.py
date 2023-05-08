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
from Kuucoin import get_query ,contract_active_entry, kuucoin_feature_ticker, kuucoin_spot_ticker, kuucoin_fund_fee_current, kuucoin_features_orders, kuucoin_spot_orders, accountEquity_check

def kucoin_exit():
  for kuucoin_entry in count():
    connection = pg.connect(
                                user = user,
                                password = pw,
                                host =  host,
                                port =  port,
                                database = database
    )
    cursor = connection.cursor()
    df_base = pd.read_sql_query(get_query(filename="kucoin_exit.sql"),connection)
    
    if len(df_base) == 0:
      print('No Exit flag')
      connection = pg.connect(
                                  user = user,
                                  password = pw,
                                  host =  host,
                                  port =  port,
                                  database = database
      )  
      cursor = connection.cursor()
      postgres_delete_query = """
                              delete from 
                                bybit_entry
                              where
                                exit_flag = '1' and
                                exchangename = 'kucoin'
                              """
      cursor.execute(postgres_delete_query)
      connection.commit()
      connection = None
      time.sleep(10)
    #elif (len(df_base) > 0) and (abs(int(df_base['nextfundingtime'].iloc[i]) - now_time) < 80) and (df_base['exit_timing'].iloc[i] == 1):
    elif len(df_base) > 0:
      for i in range(len(df_base)):
        print('Exit signal')
        connection = pg.connect(
                                  user = user,
                                  password = pw,
                                  host =  host,
                                  port =  port,
                                  database = database
        )
        cursor = connection.cursor()

        coin = str(df_base['symbol'].iloc[i])
        print(coin)
        
        ##feature order
        clientOid = str(uuid.uuid4())
        bet_price = kuucoin_feature_ticker(coin)
        bid_price_feature = bet_price[0].iloc[0]
        ask_price_feature = bet_price[1].iloc[0]
        lot_size_feature = contract_active_entry(coin)
        lot_size_feature = str(lot_size_feature.iloc[0]['lotSize'])

        leverage = '1'
        side = 'buy'
        timeInForce = 'IOC'
        type_LM = 'limit'
        kuucoin_features_orders(clientOid,
                                coin,
                                side,
                                type_LM,
                                ask_price_feature,
                                leverage,
                                lot_size_feature,
                                timeInForce
        )

        postgres_update_query = """
                                    update
                                        bybit_entry
                                    set
                                        exit_flag = '1'
                                    where
                                        symbol = %s
                                """
        cursor.execute(postgres_update_query,(coin,))
        connection.commit()

        ##spot order
        clientOid = str(uuid.uuid4())
        spot_symbol = str(df_base['symbol_spot'].iloc[i])
        bet_price_spot = kuucoin_spot_ticker(spot_symbol)
        bid_price_spot = str(bet_price_spot[0].iloc[0])
        ask_price_spot = str(bet_price_spot[1].iloc[0])
        side = 'sell'
        type_LM = 'limit'
        timeInForce = 'IOC'

        kuucoin_spot_orders(clientOid, 
                            spot_symbol, 
                            side, 
                            type_LM, 
                            ask_price_spot, 
                            lot_size_spot, 
                            timeInForce
        )

        postgres_update_query = """
                                    update
                                        bybit_entry
                                    set
                                        exit_flag = '1'
                                    where
                                        symbol = %s
                                """
        cursor.execute(postgres_update_query,(spot_symbol,))
        connection.commit()

        time.sleep(5)
        print(str(i)+'Exit done')
      connection = None
      print('Exit done')
      time.sleep(10)
    else:
      print('No timing')
      time.sleep(10)
