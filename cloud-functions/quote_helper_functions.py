#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 01:31:58 2024

@author: ianvaimberg
"""

import requests
import os
import urllib.parse
import json
from google.cloud import bigquery

def hit_vantage_api(symbol,month):
    """
    Params: symbol (stock symbol), month (relevant month of data needed)
    Returns: JSON data with OHLCV format, symbol 
    """
    
    vantage_api_key = os.getenv('alpha_vantage_API_KEY')  # Returns None if the variable isn't set
    # Need to change, but this line will have the secret API from GCP

    if vantage_api_key is None:
        raise ValueError("API key not found. Make sure it's set as an environment variable.")

    vantage_url = 'https://www.alphavantage.co/query'

    params = {'function':'TIME_SERIES_INTRADAY',
              'symbol':symbol,
              'interval':'1min',
              'month':month,
              'outputsize':'full',
              'apikey':vantage_api_key
              }

    query_string = urllib.parse.urlencode(params)

    url_with_params = f"{vantage_url}?{query_string}"

    r = requests.get(url_with_params)

    return r.json()
    

def parse_quote_json(quote_json):
    
    raw_data = quote_json['Time Series (1min)']

    symbol = quote_json['Meta Data']['2. Symbol']
        
    data_for_insert = [{'month':k[:7]+'-01', 
                        'timestamp':k,
                        'symbol_id':1,
                        'symbol':symbol,
                        'price': raw_data[k]['1. open']} for k in raw_data.keys()]
    
    return data_for_insert

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    