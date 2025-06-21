# -*- coding: utf-8 -*-
"""
Created on Sat Jun 21 12:13:49 2025

@author: kshit
"""

#from pandas.tseries.offsets import BDay
import os
#import csv
import pandas as pd
import numpy as np
import datetime as dt
#import io
#import dateutil
import pytz
from datetime import datetime

#==============================================================================
# os.chdir("Desktop\\Gap_DOWN\\Stocks")
# stocklist=pd.read_csv("Stock.csv")
# stocklist=stocklist['Stock']
# os.chdir("..\\..\\..")
#==============================================================================

#os.chdir("FNO")
#os.chdir('..')


cols=["Date","Time","Open","High","Low","Close","Volume"]
cols2=['x1','x2',"Date","Time","Open","High","Low","Close",'Volume']
report_cols=["Stock","Returns","Avg_ret"]
report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()
x=0

tz='Asia/Kolkata'

data=dict() #This is essentially a dictionary with all symbols' data
today=(datetime.now(pytz.timezone(tz))-dt.timedelta(days=21)).strftime("%Y-%m-%d")
#os.chdir("..\\Desktop\\KshitijCode")
#start=708
#end=840
#x=0

for file in os.listdir('FNO'):
    try:
        df=pd.read_csv('FNO\\'+file)
        file=file.split('.')[0]
        #df[Date_Time]=
        #data[file]=df
        #df=data[file]
        df["Date"]=np.where(df["Date"].isin(df["Date"].ix[df["Time"]==today+" 15:19:59"]),df["Date"],float('NaN'))
        df=df[np.isfinite(df["Date"])]
        df["Date"]=pd.to_datetime(df["Date"],format="%Y%m%d")
        
        df.loc[df["Time"]==today+" 15:29:59",["Day_close"]]=df.loc[df["Time"]==today+" 15:29:59",\
        ["Close"]].values
        df.loc[df["Time"]==today+" 09:19:59",["Day_open"]]=df.loc[df["Time"]==today+" 09:19:59",\
        ["Open"]].values
        df.loc[df["Time"]==today+" 15:29:59",["candle_open"]]=df.loc[df["Time"]==today+" 15:19:59",\
        ["Open"]].values
        
        df=df.sort_values(["Date","Time"])
        
        ###Next chunks of code computes the lowest lows (entry criteria) and highest highs (sl criteria) between
        ###9:15 and 9:30 and set the 9:30 lows and highs to them.
        ###This is done so that looping of the code is avoided.
        
        df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix\
        [df["Time"]==today+" 09:24:59"])),["Low"]]=np.where(df.loc[(df["Time"]==today+" 09:24:59") & \
        (df["Date"].ix[df["Time"]==today+" 09:24:59"].isin(df["Date"].ix[df["Time"]==today+" 09:19:59"])),\
        ["Low"]].values<df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"]\
        .isin(df["Date"].ix[df["Time"]==today+" 09:24:59"])),["Low"]].values ,\
        df.loc[(df["Time"]==today+" 09:24:59") & (df["Date"].ix[df["Time"]==today+" 09:24:59"].isin\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"])),["Low"]],df.loc[(df["Time"]==today+" 09:19:59")&\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix[df["Time"]==today+" 09:24:59"])),["Low"]])     
        
               
        df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix\
        [df["Time"]==today+" 09:29:59"])),["Low"]]=np.where(df.loc[(df["Time"]==today+" 09:29:59") & \
        (df["Date"].ix[df["Time"]==today+" 09:29:59"].isin(df["Date"].ix[df["Time"]==today+" 09:19:59"])),\
        ["Low"]].values<df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"]\
        .isin(df["Date"].ix[df["Time"]==today+" 09:29:59"])),["Low"]].values ,\
        df.loc[(df["Time"]==today+" 09:29:59") & (df["Date"].ix[df["Time"]==today+" 09:29:59"].isin\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"])),["Low"]],df.loc[(df["Time"]==today+" 09:19:59")&\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix[df["Time"]==today+" 09:29:59"])),["Low"]])
        
        df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix\
        [df["Time"]==today+" 09:24:59"])),["High"]]=np.where(df.loc[(df["Time"]==today+" 09:24:59") & \
        (df["Date"].ix[df["Time"]==today+" 09:24:59"].isin(df["Date"].ix[df["Time"]==today+" 09:19:59"])),\
        ["High"]].values>df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"]\
        .isin(df["Date"].ix[df["Time"]==today+" 09:24:59"])),["High"]].values ,\
        df.loc[(df["Time"]==today+" 09:24:59") & (df["Date"].ix[df["Time"]==today+" 09:24:59"].isin\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"])),["High"]],df.loc[(df["Time"]==today+" 09:19:59")&\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix[df["Time"]==today+" 09:24:59"])),["High"]])     
        
               
        df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix\
        [df["Time"]==today+" 09:29:59"])),["High"]]=np.where(df.loc[(df["Time"]==today+" 09:29:59") & \
        (df["Date"].ix[df["Time"]==today+" 09:29:59"].isin(df["Date"].ix[df["Time"]==today+" 09:19:59"])),\
        ["High"]].values>df.loc[(df["Time"]==today+" 09:19:59") & (df["Date"].ix[df["Time"]==today+" 09:19:59"]\
        .isin(df["Date"].ix[df["Time"]==today+" 09:29:59"])),["High"]].values ,\
        df.loc[(df["Time"]==today+" 09:29:59") & (df["Date"].ix[df["Time"]==today+" 09:29:59"].isin\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"])),["High"]],df.loc[(df["Time"]==today+" 09:19:59")&\
        (df["Date"].ix[df["Time"]==today+" 09:19:59"].isin(df["Date"].ix[df["Time"]==today+" 09:29:59"])),["High"]])
    
        df["Low"]=pd.to_numeric(df["Low"])
        df["High"]=pd.to_numeric(df["High"])
        df["Close"]=pd.to_numeric(df["Close"])
        df["Open"]=pd.to_numeric(df["Open"])
        df["Day_close"]=pd.to_numeric(df["Day_close"])
        df["Day_open"]=pd.to_numeric(df["Day_open"])
        df["candle_open"]=pd.to_numeric(df["candle_open"])             
        #Gap down is generated when conditions are fulfilled           
        df["Gap_down"]=np.where((df["Day_open"]<df['Day_close'].shift(1))&(df['Day_close'].shift(1)<\
        df['candle_open'].shift(1)),"Yes","")
        
        #If gap down is true, calculating threshold and SL
        df.loc[df["Gap_down"]=="Yes",["Threshold"]]=df.loc[df["Gap_down"]=="Yes",["Low"]].values
        df.loc[df["Gap_down"]=="Yes",["SL"]]=df.loc[df["Gap_down"]=="Yes",["High"]].values
        df["SL"]=pd.to_numeric(df["SL"])    
        df["Threshold"]=pd.to_numeric(df["Threshold"])
        #df['SL']=np.where(df['SL']>df['Threshold']*1.04,df['Threshold']*1.04\
        #,df['SL'])
        #df["Threshold"]=df["Threshold"]*1.0006
        
        #Calculating the days when there is indeed a gap down to loop through them so we can make a
        #moving threshold and SL for that day
        days_of_interest=df["Date"].ix[np.isfinite(df["Threshold"])]
        
        for date in days_of_interest:
            #df["Threshold"].ix[df["Date"]==date]=df["Threshold"].ix[df["Date"]==date].iloc[0]
            #threshold = df['Threshold'].loc[df['Date']==date].iloc[0]
            df.loc[df['Date']==date, 'Threshold'] = df['Threshold'].loc[df['Date']==date].iloc[0]
            #df["SL"].ix[df["Date"]==date]=df["SL"].ix[df["Date"]==date].iloc[0]
            #sl = df['SL'].loc[df['Date']==date].iloc[0]
            df.loc[df['Date']==date, 'SL']=df['SL'].loc[df['Date']==date].iloc[0]

        
        #Now we generate signals and a column for our current positions
        df["Signal"]=""

        df.loc[(np.isfinite(df["Threshold"])) & (df["Low"]<=df["Threshold"]) &\
        (df["Time"] > today+" 09:29:59"), 'Signal'] = 'Sell'        

        #df["Signal"].ix[(df["Low"]<=df["Threshold"])&(np.isfinite(df["Threshold"]))& \
        #(df["Time"]>today+" 09:29:59")]="Sell"

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
                df.loc[(df["Date"]==date)&(df["Time"]==time_of_exit), 'Exit_price'] = new_day["SL"]             
                #df["Exit_price"].ix[(df["Date"]==date)&(df["Time"]==time_of_exit)]=new_day["SL"]
            else:
                time_of_exit=new_day["Time"].iloc[-3]
                df.loc[(df["Date"]==date)&(df["Time"]==time_of_exit), 'Exit_price'] = new_day["Close"].iloc[-3]
                #df["Exit_price"].ix[(df["Date"]==date)&(df["Time"]==time_of_exit)]=new_day["Close"].\
                #iloc[-1]
            df.loc[(df["Date"]==date)&(df["Time"]>=time_of_entry)&(df["Time"]<=\
            time_of_exit), 'Entry_price'] = entry_price
            
            #df["Entry_price"].ix[(df["Date"]==date)&(df["Time"]>=time_of_entry)&(df["Time"]<=\
            #time_of_exit)]=entry_price

        #If currently not in a position already, go short when we get sell signal
        df["Exit_price"]=pd.to_numeric(df["Exit_price"])
        df["Entry_price"]=pd.to_numeric(df["Entry_price"])
        df["Returns"]=0
        df["Returns"]=np.where((df["Exit_price"]>0)&(df["Entry_price"]>0),(df["Entry_price"]-\
        df["Exit_price"])/df["Entry_price"],"")
        df["Returns"]=pd.to_numeric(df["Returns"])
        df.drop([col for col in df.columns if "Unnamed" in col], axis=1, inplace=True)
        os.chdir("320_gdrc")
        df.to_csv(file+".csv",index=False)
        os.chdir("..")
        df=df.ix[np.isfinite(df["Returns"])]
        df["Stock"]=file
        if total_df.empty:
            total_df=df
        else:
            total_df=total_df.append(df)
        if len(df)>0:
            report_df = report_df.append({'Stock': file, 'Returns':np.sum(df["Returns"]),'Avg_ret':np.sum\
            (df["Returns"])/len(df)}, ignore_index=True)
            
        print(file, 'completed successfully.')
    except:
        print('Error in', file)
        #os.chdir("..")

#os.chdir("FNO_2\\Report")
total_df["Date"]=pd.to_datetime(total_df["Date"])
total_df=total_df.sort_values("Date")
total_df.to_csv("320_gdrc\\Report\\Sell-12_stocks.csv")
report_df.to_csv("320_gdrc\\Report\\Sell_report.csv")