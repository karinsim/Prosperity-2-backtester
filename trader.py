from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List, Dict
import numpy as np
import pandas as pd

Price = int
Quantity = int


class Trader:

    def __init__(self):
        self.pos_limit = {"AMETHYSTS": 20, "STARFRUIT": 20}
        self.pos = {"AMETHYSTS": 0, "STARFRUIT": 0}
        self.st_hist = []
        self.st_count = 0
        self.st_open: Dict[Price: Quantity]
        self.st_open = {}
        
    
    def compute_orders_amth(self, order_depth, mybid, myask):
        orders: list[Order] = []
        lim = self.pos_limit["AMETHYSTS"]

        sellorders = sorted(list(order_depth.sell_orders.items()))
        buyorders = sorted(list(order_depth.buy_orders.items()), reverse=True)

        maxsell, maxbuy = len(sellorders), len(buyorders)

        total_long = 0
        total_short = 0
        if self.pos["AMETHYSTS"] < 0:
            total_short += self.pos["AMETHYSTS"]
        elif self.pos["AMETHYSTS"] > 0:
            total_long += self.pos["AMETHYSTS"]
        
        for i in range(min(maxsell, maxbuy)):
            best_ask, best_ask_amount = sellorders[i]
            best_bid, best_bid_amount = buyorders[i]
            
            if total_long < lim and \
                best_ask - mybid < 3 and not np.isnan(best_ask):
                mybuyvol = min(-best_ask_amount, lim-total_long)
                assert(mybuyvol >= 0), "Buy volume negative"
                total_long += mybuyvol
                orders.append(Order("AMETHYSTS", min(best_ask, mybid), mybuyvol))

            if total_short > -lim and \
                myask - best_bid < 3 and not np.isnan(best_bid):
                mysellvol = min(best_bid_amount, total_short+lim)
                mysellvol *= -1
                assert(mysellvol <= 0), "Sell volume positive"
                total_short += mysellvol
                orders.append(Order("AMETHYSTS", max(best_bid, myask), mysellvol))

        for j in range(i, max(maxsell, maxbuy)):
            if maxsell > maxbuy:
                best_ask, best_ask_amount = sellorders[j]
                if total_long < lim and \
                    best_ask - mybid < 3 and not np.isnan(best_ask):
                    mybuyvol = min(-best_ask_amount, lim-total_long)
                    assert(mybuyvol >= 0), "Buy volume negative"
                    total_long += mybuyvol
                    orders.append(Order("AMETHYSTS", min(best_ask, mybid), mybuyvol))
            elif maxbuy > maxsell:
                best_bid, best_bid_amount = buyorders[j]
                if total_short > -lim and myask - best_bid < 3 and not np.isnan(best_bid):
                    mysellvol = min(best_bid_amount, total_short+lim)
                    mysellvol *= -1
                    assert(mysellvol <= 0), "Sell volume positive"
                    total_short += mysellvol
                    orders.append(Order("AMETHYSTS", max(best_bid, myask), mysellvol))

        return orders


    def compute_order_st(self, state):
        orders: list[Order] = []

        order_depth = state.order_depths["STARFRUIT"]
        latest_trade = state.own_trades["STARFRUIT"]

        threshold = 0.5
        hits = 2
        mult = 2.5
        tolerance = -5  # maximum allowable loss in a trade
        maxbuy = 5

        # track open positions
        sold_last = {}
        for trade in latest_trade:
            if len(trade.buyer) > 1:
                if trade.price in self.st_open.keys():
                    self.st_open[trade.price] += trade.quantity
                else:
                    self.st_open[trade.price] = trade.quantity
                self.st_open = dict(sorted(self.st_open.items()))
            elif len(trade.seller) > 1:
                sold_last[trade.price] = trade.quantity

        # this works because you can only be long, but not short
        for soldprice in sold_last.keys():
            while sold_last[soldprice] != 0:
                topop = []
                for buyprice in self.st_open.keys():
                    if soldprice - buyprice > tolerance:
                        closed_qt = min(sold_last[soldprice], self.st_open[buyprice])
                        self.st_open[buyprice] -= closed_qt
                        sold_last[soldprice] -= closed_qt
                        if self.st_open[buyprice] == 0:
                            topop.append(buyprice)
                        
                for popped in topop:             
                    self.st_open.pop(popped)
        
        # sanity check: position
        assert sum(self.st_open.values()) == self.pos["STARFRUIT"], \
        "Open positions incorrectly tracked"
        
        sellorders = sorted(list(order_depth.sell_orders.items()))
        buyorders = sorted(list(order_depth.buy_orders.items()), reverse=True)

        best_ask, best_ask_amount = sellorders[0]
        best_bid, _ = buyorders[0]

        # assumption: midprice is available at every timestep
        mp = (best_ask + best_bid) / 2
        self.st_hist.append(mp)

        if len(self.st_hist) == 20:
            # sma_delayed = np.mean(np.array(self.st_hist)[-100:])
            sma20 = np.mean(np.array(self.st_hist)[-20:])
            upper = sma20 + mult * np.std(np.array(self.st_hist)[-20:])
            lower = sma20 - mult * np.std(np.array(self.st_hist)[-20:])
            
            # try to sell if long
            if self.pos["STARFRUIT"] > 0:
                if mp >= upper:
                    self.st_count += 1

                boughtprices = np.array(list(self.st_open.keys()))
                
                if self.st_count >= hits and \
                    np.any(best_bid - boughtprices > tolerance):
                    where = np.where(best_bid - boughtprices > tolerance)[0]
                    qty = 0
                    for w in where:
                        qty += self.st_open[boughtprices[w]]
                    orders.append(Order("STARFRUIT", best_bid, -qty))
            
            # only buy when short
            if self.pos["STARFRUIT"] <= 0:
                # reset count when exiting long position
                self.st_count = 0
            
            # uncomment this if also buying as long as below the position limit
            # if self.pos["STARFRUIT"] < self.pos_limit["STARFRUIT"]:
                # if np.abs(sma_delayed - lower) < threshold:
                if np.abs(mp - lower) < threshold:
                    buy_vol = min(-best_ask_amount,
                                  maxbuy,
                                  self.pos_limit["STARFRUIT"]-self.pos["STARFRUIT"])
                    orders.append(Order("STARFRUIT", best_ask, buy_vol))

            self.st_hist = self.st_hist[1:]

        return orders
    

    def run(self, state: TradingState):
        result = {}
        self.pos = state.position

        for product in state.order_depths:
            order_depth: OrderDepth = state.order_depths[product]
            orders: List[Order] = []

            if product == "AMETHYSTS":
                if len(order_depth.sell_orders) != 0 and \
                len(order_depth.buy_orders) != 0:
                    
                    orders = self.compute_orders_amth(order_depth, 
                                                  9999, 10001)
                    result[product] = orders
                # result[product] = []
            
            elif product == "STARFRUIT":
                if len(order_depth.sell_orders) != 0 and \
                len(order_depth.buy_orders) != 0:
                    
                    orders = self.compute_order_st(state)
                    result[product] = orders
            
            else:
                result[product] = []
                
        # traderData = "SAMPLE"
        # conversions = 1
        # return result, conversions, traderData

        return result
    

    def run_test(self, state: TradingState, orders, timestamps):
        # for testing the exchange mechanism with custom orders at corresponding timestamps

        ind = np.where(np.array(timestamps)==state.timestamp)[0][0]
        
        return orders[ind]
