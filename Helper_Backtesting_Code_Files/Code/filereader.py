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

os.chdir("..\\Users\\nextgen\\Desktop\\5 Min Data\\5 Min Data")

cols=["Date","Time","Open","High","Low","Close","Volume"]
cols2=['x1','x2',"Date","Time","Open","High","Low","Close",'Volume']
report_cols=["Stock","Returns","Avg_ret"]
report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()
x=0

tz='Asia/Kolkata'

data=dict() #This is essentially a dictionary with all symbols' data

for file in os.listdir(os.curdir):
    try:
        if x==0:
            df=pd.DataFrame(columns=cols)
        if file[-5]=='E':
            df1=pd.read_csv(file,header=1)
            df1=df1[1:]
            print(file,x)
            x=0
        else:
            x+=1
            df1=pd.read_csv(file,header=None)
        df1.columns=cols2
        df1=df1[cols]
        df=df.append(df1)
        #df["Date"]=pd.to_datetime(df["Date"],format="%Y%m%d")
        df=df.sort_values("Date")
        data[file.split('.')[0]]=df
        if x==0:
            os.chdir("Merged")
            df.to_csv(file+".csv")
            os.chdir("..")
    except:
        print(file)