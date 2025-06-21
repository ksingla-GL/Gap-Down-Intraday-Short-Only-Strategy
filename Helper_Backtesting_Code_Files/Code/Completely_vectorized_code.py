# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 16:06:11 2017

@author: nextgen
"""

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
import pytz
from datetime import datetime

os.chdir("..\\Users\\nextgen")
os.chdir("Desktop")
os.chdir("15-min_data")
os.chdir("5-min_data")
os.chdir("Data")

cols=["Date","Time","Open","High","Low","Close","Volume"]
cols2=["Date","Time","Open","High","Low","Close"]
report_cols=["Stock","Returns","Avg_ret"]
report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()
df=pd.DataFrame(columns=cols)
x=0

tz='Asia/Kolkata'

data=dict() #This is essentially a dictionary with all symbols' data
    
for file in os.listdir(os.curdir):
    try:      
        df=pd.read_csv(file,header=None)
        df=df[1:]
        try:
            df.columns=cols
        except:
            df.columns=cols2
        file=file.split("_")[0]
        #sym=df["Symbol"].loc[0]
        #df=df.ix[df["Symbol"]==sym] #This statement confines data only to near month
        #df=df.ix[df["Time"]<='15:30:59'] #Excludes data after 15:30 as that is settlement most likely
        data[file]=df
    except:
        print(file)

#Our trading rules for a sell are primarily-:
#   1.Day open< Prev Day's Close (Gap down)
#   2.Prev Day's last 15 min candle's close<open of that candle
#Thus will be using 15 min data for our backtesting
os.chdir("Total_vectorization")
today=datetime.now(pytz.timezone(tz)).strftime("%Y-%m-%d")
for file in data:
    #os.chdir("Output")
    x+=1
    df=data[file]
    df["Time"]=pd.to_datetime(df["Time"])
    df=df.ix[(df["Time"]<=today+" 15:15:00") & (df["Time"]>=today+" 09:15:00")]
    #Let us declare our new variables
    df["candle_open"]=df["Day_open"]=df["Day_close"]=df["Gap_down"]=df["Threshold"]=df["SL"]=\
    df["Signal"]=df["Entry_price"]=df["Exit_price"]=""
    #candle_open is to compare prev day's last candle's open to last candle's close
    
    #let us now get all dates in our data so we can loop through them   
    dates=df["Date"].unique()
    
    df.loc[df["Time"]==today+" 15:15:00",["Day_close"]]=df.loc[df["Time"]==today+" 15:15:00",\
    ["Close"]].values
    df.loc[df["Time"]==today+" 09:15:00",["Day_open"]]=df.loc[df["Time"]==today+" 09:15:00",\
    ["Open"]].values
    df.loc[df["Time"]==today+" 15:15:00",["candle_open"]]=df.loc[df["Time"]==today+" 15:15:00",\
    ["Open"]].values
    
    df["Low"]=pd.to_numeric(df["Low"])
    df["High"]=pd.to_numeric(df["High"])
    df["Close"]=pd.to_numeric(df["Close"])
    df["Open"]=pd.to_numeric(df["Open"])
    df["Day_close"]=pd.to_numeric(df["Day_close"])
    df["Day_open"]=pd.to_numeric(df["Day_open"])
    df["candle_open"]=pd.to_numeric(df["candle_open"])               
    #Gap down is generated when conditions are fulfilled           
    df["Gap_down"]=np.where((df["Day_open"]<df["Day_close"].shift(1))&(df["Day_close"].shift(1)<\
    df["candle_open"].shift(1)),"Yes","")
    #If gap down is true, calculating threshold and SL
    df.loc[df["Gap_down"]=="Yes",["Threshold"]]=df.loc[df["Gap_down"]=="Yes",["Low"]].values
    df.loc[df["Gap_down"]=="Yes",["SL"]]=df.loc[df["Gap_down"]=="Yes",["High"]].values
    df["SL"]=pd.to_numeric(df["SL"])    
    df["Threshold"]=pd.to_numeric(df["Threshold"])
    #df["Threshold"]=df["Threshold"]*1.0006
    
    #Calculating the days when there is indeed a gap down to loop through them so we can make a
    #moving threshold and SL for that day
    days_of_interest=df["Date"].ix[np.isfinite(df["Threshold"])]
    
    for date in days_of_interest:
        df["Threshold"].ix[df["Date"]==date]=df["Threshold"].ix[df["Date"]==date].iloc[0]
        df["SL"].ix[df["Date"]==date]=df["SL"].ix[df["Date"]==date].iloc[0]
    
    #Now we generate signals and a column for our current positions   
    df["Signal"].ix[(df["Low"]<=df["Threshold"])&(np.isfinite(df["Threshold"]))& \
    (df["Time"]>today+" 09:15:00")]="Sell"
    sell_days=df["Date"].ix[df["Signal"]!=""].unique()
    #df["Time"]=pd.to_datetime(df["Time"])
    for date in sell_days:
        no_sl=0
        new_day=df.ix[(df["Date"]==date)]
        time_of_entry=new_day["Time"].ix[(df["Signal"]=="Sell")].iloc[0]
        entry_price=new_day["Threshold"].ix[(df["Signal"]=="Sell")].iloc[0]
        time_of_exit=new_day["Time"].ix[new_day["High"]>=new_day["SL"]]
        same_candle_exit=new_day["Time"].ix[(new_day["High"]>=new_day["SL"]) & \
        (new_day["Time"]==time_of_entry)]
        if not time_of_exit.empty: #condition checks if sl is hit
            if not same_candle_exit.empty:
                time_of_exit=time_of_entry 
            elif (time_of_exit>time_of_entry).any():
                time_of_exit=time_of_exit.ix[time_of_exit>time_of_entry].iloc[0]
            else:
                #time_of_exit=new_day["Time"].iloc[-1]
                no_sl=1
        else:
            no_sl=1            
        if no_sl==0: #This means there is a stop loss triggered
            df["Exit_price"].ix[(df["Date"]==date)&(df["Time"]==time_of_exit)]=new_day["SL"]
        else:
            time_of_exit=new_day["Time"].iloc[-1]
            df["Exit_price"].ix[(df["Date"]==date)&(df["Time"]==time_of_exit)]=new_day["Close"].\
            iloc[-1]
        df["Entry_price"].ix[(df["Date"]==date)&(df["Time"]>=time_of_entry)&(df["Time"]<=\
        time_of_exit)]=entry_price
    #If currently not in a position already, go short when we get sell signal
    df["Exit_price"]=pd.to_numeric(df["Exit_price"])
    df["Entry_price"]=pd.to_numeric(df["Entry_price"])
    df["Returns"]=0
    df["Returns"]=np.where((df["Exit_price"]>0)&(df["Entry_price"]>0),(df["Entry_price"]-\
    df["Exit_price"])/df["Entry_price"],"")
    df["Returns"]=pd.to_numeric(df["Returns"])
    df.to_csv(file)
    df=df.ix[np.isfinite(df["Returns"])]
    df["Stock"]=file
    if total_df.empty:
        total_df=df
    else:
        total_df=total_df.append(df)
    report_df = report_df.append({'Stock': file, 'Returns':np.sum(df["Returns"]),'Avg_ret':np.sum\
    (df["Returns"])/len(df)}, ignore_index=True)
    #os.chdir("..")
total_df["Date"]=pd.to_datetime(total_df["Date"])
total_df=total_df.sort_values("Date")
total_df.to_csv("Sell-12_stocks.csv")
report_df.to_csv("Sell_report.csv")
