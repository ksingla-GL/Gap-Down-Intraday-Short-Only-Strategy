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

os.chdir("..\\Users\\nextgen\\Desktop\\5 Min Data\\5 Min Data\\Merged")

cols=["Date","Time","Open","High","Low","Close","Volume"]
cols2=['x1','x2',"Date","Time","Open","High","Low","Close",'Volume']
report_cols=["Stock","Returns","Avg_ret"]
report_df=pd.DataFrame(columns=report_cols)
total_df=pd.DataFrame()
x=0

tz='Asia/Kolkata'

data=dict() #This is essentially a dictionary with all symbols' data

for file in os.listdir(os.curdir):
    df=pd.read_csv(file)
    file=file.split('.')[0]
    if x==0:
        prevfile=file
    x+=1
    data[file]=df
    if prevfile==file and x>1:
        print(file)
    else:
        prevfile=file