#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 01:31:58 2024

@author: ianvaimberg
"""

import requests
import io
import os
import urllib.parse
import csv
from google.cloud import bigquery, storage
import functions_framework

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
    
    #Open symbol seed file and find id of the symbol
    #In production will need new path for symbol file
    
    symbol_id = '' # initialize to empty string
    
    
    # The below section is for local seed data reading
    # seed_path = '/Users/ianvaimberg/pairs-trading/seeds/s&p500_symbols.csv'
    
    # with open(seed_path,'r') as symbol_file:
    #     csv_reader = csv.reader(symbol_file)
        
    #     for row in csv_reader:
            
    #         if row[0] == symbol:
    #             symbol_id = row[8]
    #             break

    
    #cloud bucket seed data reading
    
    storage_client = storage.Client()
    
    bucket_name = 'seed-files'
    file_name = 'seed-files/s&p500_symbols.csv'
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name) 
    file_content = blob.download_as_text()
    csv_file = io.StringIO(file_content)
    csv_reader = csv.reader(csv_file)
    
    for row in csv_reader:
        
        if row[0] == symbol:
            symbol_id = row[8]
            break
    
    
    # parse data to achieve correct dictionary form for insert 
    
    data_for_insert = [{'month':k[:7]+'-01', 
                        'timestamp':k,
                        'symbol_id':symbol_id,
                        'symbol':symbol,
                        'price': raw_data[k]['1. open']} for k in raw_data.keys()]
    
    return data_for_insert

    
    
def insert_to_bigquery(dataset_id,table_id,data_for_insert):
    
    client = bigquery.Client()
    
    errors = client.insert_rows_json(table_id, data_for_insert)

    if errors == []:
        print("Data successfully inserted into BigQuery.")
    else:
        print(f"Errors occurred: {errors}")
    
    
def quotes_to_biqquery(symbol,month,dataset_id,table_id):
    
    raw_data = hit_vantage_api(symbol,month)
    
    parsed = parse_quote_json(raw_data)
    
    insert_to_bigquery(dataset_id,table_id,parsed)
    
@functions_framework.http 
def quotes_upload(request):
    request_json = request.get_json(silent=True)
    symbol = request_json['symbol']
    month = request_json['month']
    dataset_id = request_json['dataset_id']
    table_id = request_json['table_id']
    
    quotes_to_biqquery(symbol, month, dataset_id, table_id)   
    
    
    
    
    