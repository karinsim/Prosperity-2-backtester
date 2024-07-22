import pandas as pd
import numpy as np
from datamodel import Trade


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
    # calculate pnl as a sum of realised and unrealised pnl; for one type of product only
    # takes as input a dataframe of own trades (obtained from Prosperity log files)

    open_buy = []       # tuple (price, quantity, unrealised pnl per quantity)
    open_sell = []
    pnl_realised = 0.
    pnls = []
    dt = timestamps[1] - timestamps[0]

    for timestamp in timestamps:

        if timestamp > 0:
            # check if there was a new trade in the previous timestep
            latest_trades = mytrades.loc[mytrades["timestamp"]== timestamp - dt]
            current_price = market_price.loc[market_price["timestamp"]==timestamp]["mid_price"].unique()[0]

            for _, trade in latest_trades.iterrows():
                if trade["buyer"] == "SUBMISSION":
                    open_buy.append({
                        "price": trade["price"],
                        "quantity": trade["quantity"],
                        "unrealised": current_price - trade["price"]
                    })
                elif trade["seller"] == "SUBMISSION":
                    open_sell.append({
                        "price": trade["price"],
                        "quantity": trade["quantity"],
                        "unrealised": trade["price"] - current_price
                    })

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
        
        pnl_unrealised = 0.
        if len(open_buy) == 0 and len(open_sell) != 0:
            for sell in open_sell:
                pnl_unrealised += sell["unrealised"] * sell["quantity"]
        elif len(open_sell) == 0 and len(open_buy) != 0:
            for buy in open_buy:
                pnl_unrealised += buy["unrealised"] * buy["quantity"]
              
        pnls.append(pnl_realised + pnl_unrealised)
    
    return pnls


def aggregate_trades(trades):
    """
    Utility function to aggregate all trades with the same nature (buy/sell) and the same price into one entry.
    """

    buys, sells = [], []
    buyprices, sellprices = [], []

    for trade in trades:
        if trade.buyer == "SUBMISSION":
            if trade.price in buyprices:
                ind = np.where(np.array(buyprices)==trade.price)[0][0]
                updated_vol = trade.quantity + buys[ind].quantity
                buys[ind] = Trade(
                            symbol=trade.symbol,
                            price=trade.price,
                            quantity=updated_vol,
                            buyer="SUBMISSION",
                            seller="",
                            timestamp=trade.timestamp)
            else:
                buys.append(trade)
                buyprices.append(trade.price)
        
        elif trade.seller == "SUBMISSION":
            if trade.price in sellprices:
                ind = np.where(np.array(sellprices)==trade.price)[0][0]
                updated_vol = trade.quantity + sells[ind].quantity
                sells[ind] = Trade(
                            symbol=trade.symbol,
                            price=trade.price,
                            quantity=updated_vol,
                            buyer="",
                            seller="SUBMISSION",
                            timestamp=trade.timestamp)
            else:
                sells.append(trade)
                sellprices.append(trade.price)
       
    return buys + sells


