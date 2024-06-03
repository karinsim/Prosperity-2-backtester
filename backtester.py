import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from trader import Trader
from datamodel import OrderDepth, UserId, TradingState, Order, Listing, Trade
from typing import Dict, List

Time = int
Symbol = str
Product = str
Position = int
UserId = str
ObservationValue = int

products = ["STARFRUIT", "AMETHYSTS", "ORCHIDS", 
            "CHOCOLATE", "STRAWBERRIES", "ROSES", 
            "GIFT_BASKET", "COCONUT", "COCONUT_COUPON"]

POS_LIM = {"STARFRUIT": 20,
           "AMETHYSTS": 20,
           "ORCHIDS": 100,
           "CHOCOLATE": 250,
           "STRAWBERRIES": 350,
           "ROSES": 60,
           "GIFT_BASKET": 60,
           "COCONUT": 300,
           "COCONUT_COUPON": 600}


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


def generate_tradingstate(time, price_current, myorders, position):
    """
    Generate a TradingState object at each timestep:
    1. Iterate over each product available on the market at that timestamp
    2. Check myorders if algo has placed an order for this product 
    3. If yes, process the orders of the algo (sort in increasing/decreasing order, merge duplicates etc)
    4. Observe the position limit
    5. Match algo orders (mybuyorders/mysellorders) and market orders (listing)
    6. Process outstanding market orders & update position to put into TradingState
    * Outstanding algo orders are cancelled at the end of each iteration *
    """

    listings: Dict[Symbol, Listing] = {}
    order_depths: Dict[Symbol, OrderDepth] = {}
    own_trades: Dict[Symbol, List[Trade]] = {}
    market_trades: Dict[Symbol, List[Trade]] = {}

    for _, listing in price_current.iterrows():
        prod = listing["product"]
        
        listings[prod] = Listing(
            symbol=prod, product=prod, denomination="SEASHELLS")
        
        # process orders placed by self (the algorithm)
        mybuyorders, mysellorders = {}, {}
        
        if prod in myorders.keys():
            myorders_prod = myorders[prod]

            # check position limit is observed -- otherwise cancel all orders
            nobuy, nosell = False, False
            quantities = np.array([order.quantity for order in myorders_prod])

            if quantities[np.where(quantities > 0)[0]].sum() + position[prod] > POS_LIM[prod]:
                nobuy = True
                print("Position limit exceeded; cancelling all BUY orders")
            if quantities[np.where(quantities < 0)[0]].sum() + position[prod] < -POS_LIM[prod]:
                nosell = True
                print("Position limit exceeded; cancelling all SELL orders")

            for order in myorders_prod:
                if order.quantity > 0:
                    if not nobuy:
                        if order.price in mybuyorders.keys():
                            mybuyorders[order.price] += order.quantity
                        else:
                            mybuyorders[order.price] = order.quantity
                else:
                    if not nosell:
                        if order.price in mysellorders.keys():
                            mysellorders[order.price] += order.quantity
                        else:
                            mysellorders[order.price] = order.quantity
            
            mybuyorders = dict(sorted(mybuyorders.items(), reverse=True))
            mysellorders = dict(sorted(mysellorders.items()))
        
            # process market bid and ask orders
            buy_o, sell_o = {}, {}

            for i in range(1, 4):
                bprice = "bid_price_" + str(i)
                bvol = "bid_volume_" + str(i)
                if not np.isnan(listing[bprice]):
                    buy_o[listing[bprice]] = listing[bvol]
            for i in range(1, 4):
                aprice = "ask_price_" + str(i)
                avol = "ask_volume_" + str(i)
                if not np.isnan(listing[aprice]):
                    sell_o[listing[aprice]] = -listing[avol]            

            # match orders
            mytrades = []

            if len(mybuyorders) > 0 and len(sell_o) > 0:
                myp = list(mybuyorders.keys())[0]  
                myv = mybuyorders[myp]
                mkp = list(sell_o.keys())[0]
                mkv = -sell_o[mkp]
                
                while mkp <= myp and len(mybuyorders) > 0 and len(sell_o) > 0:
                    myp = list(mybuyorders.keys())[0]  
                    myv = mybuyorders[myp]
                    mkp = list(sell_o.keys())[0]
                    mkv = -sell_o[mkp]

                    trade_vol = min(myv, mkv)
                    trade_price = mkp

                    position[prod] += trade_vol

                    if mybuyorders[myp] - trade_vol == 0:
                        mybuyorders.pop(myp)
                    else:
                        mybuyorders[myp] -= trade_vol

                    if sell_o[mkp] + trade_vol == 0:
                        sell_o.pop(mkp)
                    else:
                        sell_o[mkp] += trade_vol

                    mytrades.append(Trade(
                        symbol=prod,
                        price=trade_price,
                        quantity=trade_vol,
                        buyer="SUBMISSION",
                        seller="",
                        timestamp=time))

            if len(mysellorders) > 0 and len(buy_o) > 0:
                myp = list(mysellorders.keys())[0]  
                myv = -mysellorders[myp]
                mkp = list(buy_o.keys())[0]
                mkv = buy_o[mkp]
                
                while mkp >= myp and len(mysellorders) > 0 and len(buy_o) > 0:
                    myp = list(mysellorders.keys())[0]  
                    myv = -mysellorders[myp]
                    mkp = list(buy_o.keys())[0]
                    mkv = buy_o[mkp]

                    trade_vol = min(myv, mkv)
                    trade_price = myp

                    position[prod] -= trade_vol

                    if mysellorders[myp] + trade_vol == 0:
                        mysellorders.pop(myp)
                    else:
                        mysellorders[myp] += trade_vol

                    if buy_o[mkp] - trade_vol == 0:
                        buy_o.pop(mkp)
                    else:
                        buy_o[mkp] -= trade_vol

                    mytrades.append(Trade(
                        symbol=prod,
                        price=trade_price,
                        quantity=trade_vol,
                        buyer="",
                        seller="SUBMISSION",
                        timestamp=time))
                
            own_trades[prod] = mytrades

            # send OUTSTANDING market orders to algo
            order_depths[prod] = OrderDepth(
                    buy_orders=buy_o, sell_orders=sell_o)
        
    return TradingState(
                 traderData="local",
                 timestamp=time,
                 listings=listings,
                 order_depths=order_depths,
                 own_trades=own_trades,
                 market_trades=market_trades,
                 position=position)

