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
import talib
import dateutil

os.chdir("..")
os.chdir("15-min_data")
os.chdir("5-min_data")
os.chdir("Output")

report_cols=["Stock","Returns","Avg_ret"]
report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()

data=dict() #This is essentially a dictionary with all symbols' data
    
for file in os.listdir(os.curdir):
    df=pd.read_csv(file)
    df["Date"]=pd.to_datetime(df["Date"])
    df=df.sort_index()
    reversal_days=df["Date"].ix[(df["Exit_price"]==df["SL"]) & (df["Time"].ix[df["Time"]<\
    '02-06-2017 14:30'])]
    if reversal_days.empty:
        reversal_days=df["Date"].ix[(df["Exit_price"]==df["SL"]) & (df["Time"].ix[df["Time"]<\
        '2017-06-02 14:30'])]
    for date in reversal_days:
        entry_index=df.ix[(df["Exit_price"]==df["SL"]) & (df["Date"]==date)].index
        df["Entry_price"].ix[(df.index>entry_index.values[0])&(df["Date"]==date)]=df["SL"].ix\
        [(df["Date"]==date)&(df["SL"]==df["Exit_price"])].values[0]
        df["SL"].ix[(df.index>entry_index.values[0])&(df["Date"]==date)]=df["Threshold"].ix[df["Date"]\
        ==date]
        if (df["Low"].ix[(df["Date"]==date)&(df.index>entry_index.values[0])]<df["SL"].ix[(df.index>\
            entry_index.values[0])&(df["Date"]==date)]).any():
            exit_index=df.ix[(df["Low"]<df["SL"])&(df["Date"]==date)&(df.index>\
            entry_index.values[0])].index
            exit_index=exit_index[0]
            df["Exit_price"].ix[df.index==exit_index]=df["SL"].ix[df.index==exit_index]
            df["Returns"].ix[df.index==exit_index]=df["SL"].ix[df.index==exit_index]/df["Entry_price"]\
            .ix[df.index==exit_index]-1
        else:
            exit_index=df.ix[df["Date"]==date].index[-1]
            df["Exit_price"].ix[df.index==exit_index]=df["Close"].ix[df.index==exit_index]
            df["Returns"].ix[df.index==exit_index]=df["Close"].ix[df.index==exit_index]/df["Entry_price"]\
            .ix[df.index==exit_index]-1


    #If currently not in a position already, go short when we get sell signal
    df["Exit_price"]=pd.to_numeric(df["Exit_price"])
    df["Entry_price"]=pd.to_numeric(df["Entry_price"])
    df["Returns"]=pd.to_numeric(df["Returns"])
    df=df.ix[np.isfinite(df["Returns"])]
    df["Stock"]=file
    os.chdir("..")
    os.chdir("Reversals")
    df.to_csv(file)
    if total_df.empty:
        total_df=df
    else:
        total_df=total_df.append(df)
    report_df = report_df.append({'Stock': file, 'Returns':np.sum(df["Returns"]),'Avg_ret':np.sum\
    (df["Returns"])/len(df)}, ignore_index=True)
    os.chdir("..")
    os.chdir("Output")
    
os.chdir("..")
os.chdir("Reversals")
total_df["Date"]=pd.to_datetime(total_df["Date"])
total_df=total_df.sort_values("Date")
total_df.to_csv("Sell-12_stocks.csv")
report_df.to_csv("Sell_report.csv")
