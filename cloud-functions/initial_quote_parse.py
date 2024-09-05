#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 16:19:41 2024

@author: ianvaimberg

Prototype of parsing file and inserting into Big Query table
"""

import json
import csv
import time 
from google.cloud import bigquery
#import pandas as pd


with open('initial_test.json','r') as file:
    dict_data = json.load(file)
    
d1 = dict_data['Time Series (1min)']

# for i, k in enumerate(d1.keys()):
    
#     if i < 10:
#         d = d1[k]
#         print({'timestamp':k,'symbol':'IBM','price':d['1. open']})
#     else:
#         break
    
data_for_insert = [{'timestamp':k,'symbol':'IBM','price':d1[k]['1. open']} for k in d1.keys()]


client = bigquery.Client()

dataset_id = 'pairs-trading-434703.test_import'
table_id = f'{dataset_id}.test_quote_data'

errors = client.insert_rows_json(table_id, data_for_insert)

if errors == []:
    print("Data successfully inserted into BigQuery.")
else:
    print(f"Errors occurred: {errors}")


# path =  '/Users/ianvaimberg/pairs-trading/seeds/s&p500_symbols.csv'
    
# with open(path,'r') as symbol_file:
#     csv_reader = csv.reader(symbol_file)
    
    
    
#     for i, row in enumerate(csv_reader):
#         if i < 10:
#             print(i, row)
#         else:
#             break
    
