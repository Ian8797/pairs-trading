#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 15:04:33 2024

@author: ianvaimberg

Purpose of this script to hit the google cloud function to populate quote history to bigquery sequentially.

We will fill history with data from 2 S&P 500 companies for 2 months as a test 
"""


base_url = "https://us-central1-pairs-trading-434703.cloudfunctions.net/quotes_to_bigquery"