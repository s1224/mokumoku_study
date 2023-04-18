import os
import ccxt as ccxt
import configparser
import pandas as pd
from pandas.io.json import json_normalize
import math
import time

conf = configparser.ConfigParser()
conf.read('./setting/setting.ini')

api_key_feature = conf['kucoin_feature']['api_key']
api_secret_feature = conf['kucoin_feature']['secret_key']
api_password = conf['kucoin_feature']['password']

api_key_spot = conf['kucoin_spot']['api_key']
api_secret_spot = conf['kucoin_spot']['secret_key']
api_password_spot = conf['kucoin_spot']['password']

kuucoin = ccxt.kucoin({"apiKey":api_key_feature,"secret":api_secret_feature,"password":api_password})
kuucoin_spot = ccxt.kucoin({"apiKey":api_key_spot,"secret":api_secret_spot,"password":api_password_spot})

##database
def get_query(filename: str)->str:
    with open(os.path.join("./sql",filename), "r") as f:
        return f.read()

##kuucoin
def contract_active():
  contracts = kuucoin_feature.futuresPublic_get_contracts_active()
  contracts = pd.json_normalize(contracts['data'])
  #filter = contracts['symbol'] == symbol_name
  #contracts = contracts[filter]
  return contracts

def kuucoin_fund_fee_current(symbol_name):
  current_fee = kuucoin_feature.futuresPublic_get_funding_rate_symbol_current(params = {
                                                                                        'symbol':symbol_name
  })
  current_fee = pd.json_normalize(current_fee['data'])
  return current_fee
