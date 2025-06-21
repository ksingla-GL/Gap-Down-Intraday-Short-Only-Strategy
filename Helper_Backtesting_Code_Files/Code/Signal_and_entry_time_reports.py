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
import math

os.chdir("..\\Users\\nextgen")
os.chdir("Desktop")
os.chdir("15-min_data")
os.chdir("5-min_data")
os.chdir("Data")
os.chdir("Total_Vectorization")

#report_cols=["Stock","Returns","Avg_ret"]
#report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()
    
for file in os.listdir(os.curdir):
    try:
        df=pd.read_csv(file)
        df["Date"]=pd.to_datetime(df["Date"])
        df=df.sort_index()
        #If currently not in a position already, go short when we get sell signal
        df["Exit_price"]=pd.to_numeric(df["Exit_price"])
        df["Entry_price"]=pd.to_numeric(df["Entry_price"])
        df["Returns"]=pd.to_numeric(df["Returns"])
        df["Stock"]=file.split(".")[0]
        Entry_time=df[["Date","Time","Stock"]].ix[(df["Entry_price"]>0)&(df["Entry_price"].shift(1)!=\
        df["Entry_price"].shift(1))]
        Entry_time.index=Entry_time["Date"]
        
        Signal_days=df["Date"].ix[df["Gap_down"]=="Yes"].unique()
        Signal_days=pd.DataFrame(Signal_days)
        Signal_days.columns=["Date"]
        Signal_days["Stock"]=file.split(".")[0]
        Signal_days.index=Signal_days["Date"]
        Signal_days["Entry_Time"]=Entry_time["Time"].ix[Entry_time["Date"].isin(Signal_days["Date"])]
        df.index=df["Date"]
        Signal_days["Return"]=df["Returns"].ix[(df["Date"].isin(Signal_days["Date"]))&(df["Returns"]==
        df["Returns"])]
        Signal_days=Signal_days[["Stock","Entry_Time","Return"]]
        os.chdir("Report")
        #df.to_csv(file)
        if total_df.empty:
            total_df=Signal_days
        else:
            total_df=total_df.append(Signal_days)
        os.chdir("..")
    except:
        print(file)
    
os.chdir("Report")
#total_df["Date"]=pd.to_datetime(total_df["Date"])
#total_df=total_df.sort_values("Date")
total_df.to_csv("Signal_Entry.csv")
