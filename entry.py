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
import uuid

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


    if len(df_base) == 0:
      print('No data')
      time.sleep(5)
    elif len(df_base_check) == 0:
      print('Entry duplication')
      time.sleep(5)
    #elif len(df_base) > 0 and ((now_time - int(df_base.iloc[0]['timepoint'])) < 60000000000000000):
    elif len(df_base) > 0:
      print('Entry')
      # now_time = int(math.floor(time.time() * 1 * 10 ** 0) /(10 ** 0))
      # now_time_ = int(df_base.iloc[0]['timepoint_rev'])
      # now_time = abs(now_time - now_time_)
      bet_price = kuucoin_feature_ticker('BCHUSDTM')
      bid_price_feature = bet_price[0].iloc[0]
      if accountEquity_check('USDT') > bid_price_feature:
        for i in range(len(df_base)):
          #bid = sell, ask = buy
          ##order feature
          symbol = str(df_base.iloc[i]['symbolname'])
          clientOid = str(uuid.uuid4())
          bet_price = kuucoin_feature_ticker(symbol)
          bid_price_feature = bet_price[0].iloc[0]
          ask_price_feature = bet_price[1].iloc[0]
          lot_size_feature = contract_active_entry(symbol)
          lot_size_spot = str(lot_size_feature.iloc[0]['indexPriceTickSize'])
          lot_size_feature = str(lot_size_feature.iloc[0]['lotSize'])

          leverage = '1'
          side = 'sell'
          timeInForce = 'IOC'
          type_LM = 'limit'
          ##order
          kuucoin_features_orders(clientOid,
                                  symbol_name,
                                  side,
                                  type_LM,
                                  bid_price_feature,
                                  leverage,
                                  lot_size_feature,
                                  timeInForce
          )

          ##order spot
          clientOid = str(uuid.uuid4())
          spot_symbol = str(df_base.iloc[i]['spot_symbol'])
          bet_price_spot = kuucoin_spot_ticker(spot_symbol)
          bid_price_spot = str(bet_price_spot[0].iloc[0])
          ask_price_spot = str(bet_price_spot[1].iloc[0])
          side = 'buy'
          type_LM = 'limit'
          timeInForce = 'IOC'
          ##order
          kuucoin_spot_orders(clientOid, 
                              spot_symbol, 
                              side, 
                              type_LM, 
                              ask_price_spot, 
                              lot_size_spot, 
                              timeInForce
          )
          
          #feature data insert
          connection = pg.connect(
                                user = user,
                                password = pw,
                                host =  host,
                                port =  port,
                                database = database
          )
          cursor = connection.cursor()

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

          
          symbol = symbol
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

          # #spot data insert
          postgres_insert_query = """
                                      insert into
                                          bybit_entry(
                                                      nextfundingtime
                                                      ,exit_flag
                                                      ,exchangename
                                                      ,symbol
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
          symbol_spot = spot_symbol
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
        print('Order')
      else:
        print('Insufficient balance')
    else:
      print('other')
      time.sleep(3)
