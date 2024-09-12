#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 11:35:57 2024

@author: ianvaimberg

Adding an id column to the symbol data for partition on Bigquery 
"""
import csv 


path = 's&p500_symbols.csv'


with open(path,'r') as symbol_file:
    csv_reader = csv.reader(symbol_file)
    
    new_data = [r + (['symbol_id'] if i == 0 else [i-1]) for i,r in enumerate(csv_reader)]
    

new_data[0] = ['symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'CIK', 'Founded', 'symbol_id']

with open(path,'w') as symbol_file:
    
    csv_writer = csv.writer(symbol_file)
    
    csv_writer.writerows(new_data)

