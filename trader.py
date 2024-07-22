from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List
import numpy as np


class Trader:

    def __init__(self):
        self.pos_limit = {"AMETHYSTS": 20, "STARFRUIT": 20}
        self.pos = {"AMETHYSTS": 0, "STARFRUIT": 0}

        self.st_hist = [4899, 4899.5, 4898.5, 4900, 4902.5, 4899.5, 
                        4899.5, 4901, 4898.5, 4901.5, 4901, 4900.5, 
                        4900, 4899.5, 4901.5, 4898.5, 4903.5, 4900.5, 
                        4900.5, 4903, 4900.5, 4899.5, 4900, 4903, 4900.5, 
                        4899.5, 4901, 4901, 4898.5, 4903.5, 4901, 4901,
       4900.5, 4901.5, 4901. , 4901. , 4899.5, 4900. , 4901. , 4899.5,
       4900.5, 4902.5, 4900. , 4899. , 4899.5, 4900. , 4899. , 4896.5,
       4899. , 4899.5, 4899.5, 4899.5, 4899.5, 4899.5, 4899.5, 4899.5,
       4901.5, 4899. , 4902.5, 4901. , 4900.5, 4900. , 4899. , 4897.5,
       4899. , 4899.5, 4899.5, 4899.5, 4897. , 4898.5, 4898. , 4901. ,
       4898.5, 4896.5, 4897.5, 4896.5, 4897.5, 4896.5, 4896.5, 4896.5,
       4895.5, 4895.5, 4891.5, 4895. , 4894.5, 4892.5, 4895.5, 4898. ,
       4894.5, 4896. , 4896. , 4895.5, 4896. , 4896. , 4895.5, 4897.5,
       4895. , 4897. , 4894. , 4896.5, 4896.5, 4896.5, 4896.5, 4897. ,
       4899.5, 4897. , 4896.5, 4898. , 4897.5, 4897.5, 4901. , 4901.5,
       4899. , 4898.5, 4899. , 4901. , 4900.5, 4900.5, 4900.5, 4898. ,
	  4900.5, 4900.5, 4901. , 4900.5, 4901. , 4900.5, 4900.5, 4900. ,
       4900.5, 4902. , 4902. , 4901. , 4900.5, 4901. , 4901. , 4899.5,
       4901.5, 4901.5, 4902. , 4901.5, 4901.5, 4898.5, 4902. , 4904. ,
       4903. , 4902. , 4901.5, 4902.5, 4902.5, 4902.5, 4902.5, 4903. ,
       4902.5, 4903. , 4903. , 4903.5, 4905. , 4904.5, 4904.5, 4906. ,
       4905. , 4906. , 4905.5, 4906. , 4906. , 4906. , 4906.5, 4902.5,
       4905. , 4903. , 4905. , 4905. , 4904.5, 4904.5, 4904.5, 4905.5,
       4906.5, 4908. , 4908. , 4904.5, 4908. , 4908. , 4908. , 4907.5,
       4907.5, 4907.5, 4908. , 4909. , 4909.5, 4911. , 4907.5, 4906.5,
       4907.5, 4907. , 4907. , 4907. , 4906.5, 4906.5, 4908. , 4909. ]


    def compute_orders_amth(self, order_depth, mybid, myask):
        orders: list[Order] = []
        lim = self.pos_limit["AMETHYSTS"]

        sellorders = sorted(list(order_depth.sell_orders.items()))
        buyorders = sorted(list(order_depth.buy_orders.items()), reverse=True)

        maxsell, maxbuy = len(sellorders), len(buyorders)

        for i in range(min(maxsell, maxbuy)):
            best_ask, best_ask_amount = sellorders[i]
            best_bid, best_bid_amount = buyorders[i]
            
            if self.pos["AMETHYSTS"] < lim and \
                best_ask - mybid < 3 and not np.isnan(best_ask):
                mybuyvol = min(-best_ask_amount, lim-self.pos["AMETHYSTS"])
                assert(mybuyvol >= 0), "Buy volume negative"
                self.pos["AMETHYSTS"] += mybuyvol
                orders.append(Order("AMETHYSTS", min(best_ask, mybid), mybuyvol))

            if self.pos["AMETHYSTS"] > -lim and \
                myask - best_bid < 3 and not np.isnan(best_bid):
                mysellvol = min(best_bid_amount, self.pos["AMETHYSTS"]+lim)
                mysellvol *= -1
                assert(mysellvol <= 0), "Sell volume positive"
                self.pos["AMETHYSTS"] += mysellvol
                orders.append(Order("AMETHYSTS", max(best_bid, myask), mysellvol))

        for j in range(i, max(maxsell, maxbuy)):
            if maxsell > maxbuy:
                best_ask, best_ask_amount = sellorders[j]
                if self.pos["AMETHYSTS"] < lim and \
                    best_ask - mybid < 3 and not np.isnan(best_ask):
                    mybuyvol = min(-best_ask_amount, lim-self.pos["AMETHYSTS"])
                    assert(mybuyvol >= 0), "Buy volume negative"
                    self.pos["AMETHYSTS"] += mybuyvol
                    orders.append(Order("AMETHYSTS", min(best_ask, mybid), mybuyvol))
            elif maxbuy > maxsell:
                best_bid, best_bid_amount = buyorders[j]
                if self.pos["AMETHYSTS"] > -lim and myask - best_bid < 3 and not np.isnan(best_bid):
                    mysellvol = min(best_bid_amount, self.pos["AMETHYSTS"]+lim)
                    mysellvol *= -1
                    assert(mysellvol <= 0), "Sell volume positive"
                    self.pos["AMETHYSTS"] += mysellvol
                    orders.append(Order("AMETHYSTS", max(best_bid, myask), mysellvol))

        return orders


    def compute_order_st(self, order_depth):
        orders: list[Order] = []
        lim = self.pos_limit["STARFRUIT"]

        sellorders = sorted(list(order_depth.sell_orders.items()))
        buyorders = sorted(list(order_depth.buy_orders.items()), reverse=True)

        best_ask, best_ask_amount = sellorders[0]
        best_bid, best_bid_amount = buyorders[0]

        cp = (best_ask + best_bid) / 2
        self.st_hist.append(cp)
        self.st_hist = self.st_hist[1:]
        assert len(self.st_hist) == 200, "Incorrect historical data length"
        
        # SMA strategy -- alternatively use EMA
        shorter_below = False
        sma200 = np.mean(np.array(self.st_hist)[-50:])
        sma50 = np.mean(np.array(self.st_hist)[-20:])
        sma20 = np.mean(np.array(self.st_hist)[-5:])

        shorter_below = False
        if sma50 > sma20:
            if not shorter_below and cp < sma200 and \
               sma20 < 200 and sma50 < 200 and self.pos["STARFRUIT"] > -lim:
                mysellvol = min(best_bid_amount, self.pos["STARFRUIT"]+lim)
                mysellvol *= -1
                assert(mysellvol <= 0), "Sell volume positive"
                self.pos["STARFRUIT"] += mysellvol
                orders.append(Order("STARFRUIT", best_bid, mysellvol))
            shorter_below = True

        elif sma50 < sma20 :
            if shorter_below and cp > sma200 and \
                sma20 > 200 and sma50 > 200 and self.pos["STARFRUIT"] < lim:
                mybuyvol = min(-best_ask_amount, lim-self.pos["STARFRUIT"])
                assert(mybuyvol >= 0), "Buy volume negative"
                self.pos["STARFRUIT"] += mybuyvol
                orders.append(Order("STARFRUIT", best_ask, mybuyvol))
            shorter_below = False

        return orders
    

    def run(self, state: TradingState):
        result = {}

        for product in state.order_depths:
            order_depth: OrderDepth = state.order_depths[product]
            orders: List[Order] = []

            if product == "AMETHYSTS":
                if len(order_depth.sell_orders) != 0 and \
                len(order_depth.buy_orders) != 0:
                    
                    orders = self.compute_orders_amth(order_depth, 
                                                  9999, 10001)
                    result[product] = orders
            
            else:
                result[product] = []
            
            # if product == "STARFRUIT":
            #     if len(order_depth.sell_orders) != 0 and \
            #     len(order_depth.buy_orders) != 0:
                    
            #         orders = self.compute_order_st(order_depth)
            #         result[product] = orders
                
        # traderData = "SAMPLE"
        # conversions = 1
        # return result, conversions, traderData

        return result
    

    def run_test(self, state: TradingState, orders, timestamps):
        # for testing the exchange mechanism with custom orders at corresponding timestamps

        ind = np.where(np.array(timestamps)==state.timestamp)[0][0]
        
        return orders[ind]
