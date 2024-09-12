#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 12:22:22 2024

@author: ianvaimberg
"""


from quote_helper_functions import quotes_to_biqquery

symbol = 'IBM'
month = '2024-08'
dataset_id = 'pairs-trading-434703.raw_historical_quotes'
table_id = f'{dataset_id}.quote_data'

quotes_to_biqquery(symbol,month,dataset_id,table_id)

