#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 12:11:54 2024

@author: ianvaimberg
"""

import requests
import os
import urllib.parse
import json

vantage_api_key = os.getenv('alpha_vantage_API_KEY')  # Returns None if the variable isn't set

if vantage_api_key is None:
    raise ValueError("API key not found. Make sure it's set as an environment variable.")

vantage_url = 'https://www.alphavantage.co/query'

params = {'function':'TIME_SERIES_INTRADAY',
          'symbol':'IBM',
          'interval':'1min',
          'month':'2024-08',
          'outputsize':'full',
          'apikey':vantage_api_key
          }

query_string = urllib.parse.urlencode(params)

url_with_params = f"{vantage_url}?{query_string}"

r = requests.get(url_with_params)

data = r.json()

print(len(data['Time Series (1min)']))

with open('initial_test.json','w') as file:
    
    json.dump(data,file)
