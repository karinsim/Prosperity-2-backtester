import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from trader import Trader
from datamodel import OrderDepth, UserId, TradingState, Order, Listing
from typing import Dict, List

Time = int
Symbol = str
Product = str
Position = int
UserId = str
ObservationValue = int


def get_tradehistory(file):
    # extract trade history from raw log output
    fp = open(file)
    start = False
    cont = ["[", "]", "{"]
    hist = []
    empty = {}

    for line in fp:
        flag = False
        if start:
            for c in cont:
                if c in line:
                    flag = True
            
            if not flag:
                if "}" in line:
                    hist.append(empty)
                    empty = {}
                    continue
                
                item1 = line.split()[0][1:-2]
                item2 = line.split()[1].replace(",", "").replace('"', "")
                if item2.isnumeric():
                    item2 = int(item2)
                
                toadd = {item1: item2}
                empty.update(toadd)

        elif "Trade History" in line:
            start = True

    fp.close()
    
    return pd.DataFrame(hist)


def get_mytrades(hist, prod="AMETHYSTS"):
    # extract own trades from a record of all market trades
    mytrades = []
    
    for _, h in hist.iterrows():
        if h["symbol"] == prod:
            if h["seller"] == "SUBMISSION" or h["buyer"] == "SUBMISSION":
                mytrades.append(h)
    
    return pd.DataFrame(mytrades)


def get_pnl(mytrades, timestamps, market_price):
    # calculate pnl as a sum of realised and unrealised pnl

    open_buy = []
    open_sell = []
    pnl_realised = 0.
    pnl_unrealised = 0.
    pnls = []
    calc_unrealised = False

    for timestamp in timestamps:

        # Realized PnL
        while len(open_buy) != 0 and len(open_sell) != 0:
            buy = open_buy[0]
            sell = open_sell[0]
            close_qty = min(buy["quantity"], sell["quantity"])
            pnl_realised += (sell["price"] - buy["price"]) * close_qty
            buy["quantity"] -= close_qty
            sell["quantity"] -= close_qty

            if buy["quantity"] == 0:
                open_buy = open_buy[1:]
            if sell["quantity"] == 0:
                open_sell = open_sell[1:]
        
        # Unrealized PnL
        if calc_unrealised:
            pnl_unrealised = 0.
            marktpr = market_price.loc[market_price["timestamp"] == 
                                    timestamp]["mid_price"].unique()[0]
            
            if len(open_buy) != 0:
                for buy in open_buy:
                    pnl_unrealised += buy["quantity"] * (marktpr - buy["price"])

            elif len(open_sell) != 0:
                for sell in open_sell:
                    pnl_unrealised += sell["quantity"] * (sell["price"] - marktpr)
            
            calc_unrealised = False

        # if there is a change in position, re-calculate unrealised pnl in the next timestep
        current_trades = mytrades.loc[mytrades["timestamp"]==timestamp]
        if len(current_trades.index) > 0:
            calc_unrealised = True
        
        for _, trade in current_trades.iterrows():
            if trade["buyer"] == "SUBMISSION":
                open_buy.append({
                    "price": trade["price"],
                    "quantity": trade["quantity"]
                })
            elif trade["seller"] == "SUBMISSION":
                open_sell.append({
                    "price": trade["price"],
                    "quantity": trade["quantity"]
                })
              
        pnls.append(pnl_realised + pnl_unrealised)
    
    return pnls