# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 15:05:37 2017

@author: nextgen
"""

import datetime as dt
import pandas as pd
import numpy as np
import os
import csv

os.chdir("TWS API\\source\\pythonclient")

from ibapi.client import EClient
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import *
from ibapi.tag_value import TagValue

from datetime import datetime
import pytz # $ pip install pytz

tz='Asia/Kolkata'

time=datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S")              #09:30:00

gap_down_reckoning="09:15:00"  #The opening time of the market
gd_reckoning_close_time="09:30:00"   #The close of the first candle of the day 
square_off_time="15:25:00" #The time at which the positions will be square off,before close
rc_reckoning_close_time="15:30:00" #The closing time of the market


class TestApp(EWrapper, EClient):
    
    def __init__(self):
        EClient.__init__(self, self)
        
        day=datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y")
        stocklist=pd.read_csv("trades_data\\Red_candles\\"+day+".csv")
        self.contracts = []            #checks if stock is gap down and red candle prev day. False if so.
        self.bar_time = None
        self.last_candle_close=0
        self.last_candle_open=0
        self.bar_high=dict.fromkeys(stocklist['Stock'],0)
        self.bar_low=dict.fromkeys(stocklist['Stock'],0)
        self.check=False
        
    def tickPrice(self, reqId , tickType, price:float, attrib):
            
        #if tickType == 1:
            #print('Bid:', price)
        #elif tickType == 2:
            #print('Ask:', price)
        self.time=datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S")
        c = self.contracts[reqId]
        #reqId=0
        #print(self.time)
        if tickType == 4: #This is LTP
            print(c.symbol+"->"+str(price))
            #self.cancelMktData(reqId)
            if price>self.bar_high[c.symbol]:
                self.bar_high[c.symbol]=price
            if price<self.bar_low[c.symbol]:
                self.bar_low[c.symbol]=price
            status=pd.read_csv("trades_data\\Status.csv")
            trades=pd.read_csv("trades_data\\Trades_sheet\\"+\
            datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y")+".csv")
            #if c in status["Stock"]:
            entry_price=status["Entry_price"].ix[status["Stock"]==c.symbol].iloc[-1]
            exit_price=status["Exit_price"].ix[status["Stock"]==c.symbol].iloc[-1]
            
            try:
                print(c.symbol+": LTP="+str(price)+", Entry Price="+str(entry_price)+", Exit Price="+str(exit_price))
            except:
                print("Exception in print")
            #else:
            #self.cancelMktData(reqId)
            if (status['state'].ix[status["Stock"]==c.symbol]).all()=='off':
                self.cancelMktData(reqId)
            
            if self.time >= gap_down_reckoning and (status['First_Open_bar'].ix[status['Stock']\
            ==c.symbol]==False).any() and self.time < gd_reckoning_close_time:
                status['First_Open_bar'].ix[status['Stock']==c.symbol]="True"
                c = self.contracts[reqId]
                prev_data = pd.read_csv('Prev_Day\\India\\'+c.symbol+'.csv')
    
                if price >= prev_data['Close'].iloc[-1] or price >= \
                (prev_data['Price_avg'].iloc[-1]*99+price)/100:
                    print(c.symbol+" has not opened gap down.")
                    self.cancelMktData(reqId)
                    status['state'].ix[status['Stock'] == c.symbol]='off'
                else:
                    print(c.symbol+" has OPENED gap down.")
                    self.bar_high[c.symbol]=price
                    self.bar_low[c.symbol]=price
                    self.check=True
                
            elif self.time >= gd_reckoning_close_time and self.time < square_off_time:
                if (status['First_Close_bar'].ix[status['Stock']==c.symbol]==False).any() and\
                (status['state'].ix[status["Stock"]==c.symbol]).all()=='on':
                    status['Threshold_price'].ix[status['Stock']==c.symbol] = \
                    self.bar_low[c.symbol]
                    status['SL_price'].ix[status['Stock']==c.symbol] = self.bar_high[c.symbol]
                    
                    f=open("ISO-signal.txt", "a+")
                    
                    s = str(c.symbol)+","+"S"+","+"1"+","+str(self.bar_low[c.symbol])+","\
                    +str(self.bar_high[c.symbol])+","+str(self.bar_low[c.symbol]*0.8)+","\
                    +str(datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y"))+","\
                    +str(datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S"))+","+"ISO"\
                    +"\n"
                
                    f.write(s)
                    #f.write(c.symbol,"S",'1',self.bar_low[c.symbol],self.bar_high[c.symbol],\
                    #self.bar_low[c.symbol]*0.8,datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y"),\
                    #datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S"),"ISO")
                    
                    f.close()
                    
                    status['First_Close_bar'].ix[status['Stock']==c.symbol]="True"
                
                if price<status['Threshold_price'].ix[status['Stock']==c.symbol].iloc[-1] and\
                entry_price==0:
                    print(str(datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y %H:%M:%S")))
                    print("Short "+c.symbol+"now at price of "+str(status['Threshold_price'].\
                    ix[status['Stock']==c.symbol].iloc[-1]))
                    status["Entry_price"].ix[status["Stock"]==contracts[reqId].symbol]=\
                    status['Threshold_price'].ix[status['Stock']==c.symbol]
                    
                    trades=trades.append({'Date':datetime.now(pytz.timezone(tz)).\
                    strftime("%d-%m-%y"),'Stock':c.symbol,'Entry_price':\
                    status['Threshold_price'].ix[status['Stock']==c.symbol].iloc[-1],"Entry_Time":\
                    datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S"),\
                    "Exit_price":0},ignore_index=True)
                     
    
                elif entry_price>0 and price>status['SL_price'].ix[status['Stock']==\
                c.symbol].iloc[-1]:
                    
                    print("SL Buy "+c.symbol+"now at price of "+str(status['SL_price'].\
                    ix[status['Stock']==c.symbol].iloc[-1]))
                    status["Exit_price"].ix[status["Stock"]==c.symbol]=\
                    exit_price=status['SL_price'].ix[status['Stock']==c.symbol].iloc[-1]
                    
                    trades["Exit_price"].ix[trades["Stock"]==c.symbol]=\
                    status['SL_price'].ix[status['Stock']==c.symbol].iloc[-1]
                    trades["Exit_Time"].ix[trades["Stock"]==contracts[reqId].symbol]=\
                    datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S")
                
            if self.time >= square_off_time and (status['Square_off_open'].ix[status['Stock']\
            ==c.symbol]==False).any() and self.time < rc_reckoning_close_time:
                print("Square off now "+c.symbol+"now at price of "+str(price))
                status['Square_off_open'].ix[status['Stock']==c.symbol]="True"
                f=open("ISO-signal.txt", "w+")
                f.close()
                
                if (trades["Exit_price"].ix[trades["Stock"]==c.symbol]==0).any():
                    trades["Exit_price"].ix[trades["Stock"]==c.symbol]=price
                    trades["Exit_Time"].ix[trades["Stock"]==c.symbol]=\
                    datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S")
                    self.cancelMktData(reqId)
                            
            if self.time >= rc_reckoning_close_time:
                self.cancelMktData(reqId)
                            
            if exit_price>0:
                self.cancelMktData(reqId)
            status.to_csv("trades_data\\Status.csv",index=False)
            trades.to_csv("trades_data\\Trades_sheet\\"+\
            datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y")+".csv",index=False)

        elif self.check==True and tickType==8:
            print("Checking vol avg for "+c.symbol+" now.")
            if price<=(prev_data['Vol_avg'].iloc[-1]*99+price)/100:
                self.cancelMktData(reqId)
                self.check=False
                
app = TestApp()
app.connect('127.0.0.1', 8100, 0)

time=datetime.now(pytz.timezone(tz)).strftime("%H:%M:%S")
day=datetime.now(pytz.timezone(tz)).strftime("%d-%m-%y")

contracts=[]
stocklist=pd.read_csv("trades_data\\Red_candles\\"+day+".csv")
for stock in stocklist['Stock']:
    c = Contract()
    c.symbol = stock
    c.secType = 'STK'
    c.exchange = 'NSE'
    c.currency = 'INR'
    contracts.append(c)
    
app.contracts = contracts
#i=0
#app.reqRealTimeBars(i, contracts[i], 5, 'TRADES', True, [])

#==============================================================================
# contracts=[]
# contracts.append(c)
# app.contracts=contracts
# app.reqMktData(0, c, '', False, False, [])
#==============================================================================

#I create two dataframes(stored as dynamic csv files) which are nearly identical- status and trades sheet. 
#Status is to monitor status of all red candle stocks and trades records all trades.

trade_cols=["Date","Entry_Time","Stock","Entry_price","Exit_price","Exit_Time","Returns"]
trades_df=pd.DataFrame(columns=trade_cols)
status_cols=["Date","Stock","First_Open_bar","First_Close_bar","Second_Open_bar",\
"Square_off_open",'Threshold_price','Exit_price',"Entry_price","SL_price","state"]
status_df=pd.DataFrame(columns=status_cols)
single_request=False

    #print(time)       
for i in range(len(contracts)):
    #try:
    app.reqMktData(i, contracts[i], '', False, False, [])
    #except:
    #print("There was an error retrieving data for "+contracts[i].symbol)
    status_df=status_df.append({'Date':day,'Stock':contracts[i].symbol,'Entry_price':0,\
    'Exit_price':0,'First_Open_bar':"False",'First_Close_bar':"False",'Second_Open_bar':"False",\
    'Square_off_open':"False",'Threshold_price':0,'SL_price':0,'state':'on'},ignore_index=True)
    
status=pd.read_csv("trades_data\\Status.csv")
if status['Date'].iloc[0]!=day:
    print("OK")    
    status_df.to_csv("trades_data\\Status.csv",index=False)
    trades_df.to_csv("trades_data\\Trades_sheet\\"+day+".csv",index=False)
        
    #print("Python server time-> "+time)

app.run()