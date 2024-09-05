#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 16:19:41 2024

@author: ianvaimberg
"""

import json
#import pandas as pd


with open('initial_test.json','r') as file:
    dict_data = json.load(file)
    


d1 = dict_data['Time Series (1min)']

for i, k in enumerate(d1.keys()):
    
    if i < 10:
        print(d1[k])
    else:
        break