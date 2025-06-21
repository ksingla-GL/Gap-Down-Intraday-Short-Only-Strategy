# -*- coding: utf-8 -*-
"""
Created on Wed May 31 14:36:01 2017

@author: nextgen
"""

from pandas.tseries.offsets import BDay
import os
import csv
import pandas as pd
import numpy as np
import datetime as dt
import io
import dateutil

os.chdir("..\\Users\\nextgen")
os.chdir("Desktop")
os.chdir("15-min_data")
os.chdir("5-min_data")
#os.chdir("30-Stock")
os.chdir("Data\\Total_Vectorization")

report_cols=["Stock","Returns","Avg_ret"]
report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()
    
for file in os.listdir(os.curdir):
    try:
        df=pd.read_csv(file)
        df["Date"]=pd.to_datetime(df["Date"])
        df=df.sort_index()
        df["mavg"]=df["Volume"].rolling(window=100).mean()
        mavg_days=df["Date"].ix[(df["Volume"]>df["mavg"]) & (df["Gap_down"]=="Yes")]
        df=df.ix[df["Date"].isin(mavg_days)]
        #If currently not in a position already, go short when we get sell signal
        df["Exit_price"]=pd.to_numeric(df["Exit_price"])
        df["Entry_price"]=pd.to_numeric(df["Entry_price"])
        df["Returns"]=pd.to_numeric(df["Returns"])
        df=df.ix[np.isfinite(df["Returns"])]
        df["Stock"]=file
        os.chdir("Vol")
        df.to_csv(file)
        if total_df.empty:
            total_df=df
        else:
            total_df=total_df.append(df)
        report_df = report_df.append({'Stock': file, 'Returns':np.sum(df["Returns"]),'Avg_ret':np.sum\
        (df["Returns"])/len(df)}, ignore_index=True)
        os.chdir("..")
    except:
        print(file)
    
os.chdir("Vol")
total_df["Date"]=pd.to_datetime(total_df["Date"])
total_df=total_df.sort_values("Date")
total_df.to_csv("Sell-12_stocks.csv")
report_df.to_csv("Sell_report.csv")
