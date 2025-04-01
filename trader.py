from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List
import string
import numpy as np


class Trader:
    def __init__(self):
        self.POS_LIM = {"AMETHYSTS": 20, "STARFRUIT": 20}
        self.prods = ["AMETHYSTS", "STARFRUIT"]
        self.open_buys = {prod: {} for prod in self.prods}
        self.open_sells = {prod: {} for prod in self.prods}
        self.recorded_time = -1     # last recorded time of own_trades


    def update_open_pos(self, state: TradingState):
        """
        Update open positions according to updated own trades
        Later try to buy/sell lower/higher than open trades
        """

        for prod in state.own_trades:
            sold_price = sorted(list(self.open_sells[prod].keys()), reverse=True)
            trades = state.own_trades[prod]

            if trades[0].timestamp > self.recorded_time:
                for trade in trades:
                    remaining_quantity = trade.quantity
                    if trade.buyer == "SUBMISSION":
                        # match with currently open positions
                        for price in sold_price:  
                            if trade.price >= price: 
                                break  
                            if remaining_quantity <= 0:
                                break  
                            
                            if price in self.open_sells[prod]:
                                available_quantity = self.open_sells[prod][price]
                                if remaining_quantity >= available_quantity:  
                                    remaining_quantity -= available_quantity  
                                    del self.open_sells[prod][price]  
                                else:  
                                    self.open_sells[prod][price] -= remaining_quantity  
                                    remaining_quantity = 0
                            else:
                                continue
                        if remaining_quantity > 0:
                            if trade.price in self.open_buys[prod]:
                                self.open_buys[prod][trade.price] += remaining_quantity
                            else:
                                self.open_buys[prod][trade.price] = remaining_quantity
                            
                            
                    else:
                        bought_price = sorted(list(self.open_sells[prod].keys()))
                        for price in bought_price:  
                            if trade.price <= price: 
                                break  
                            if remaining_quantity <= 0:
                                break 

                            if price in self.open_buys[prod]:
                                available_quantity = self.open_buys[prod][price]
                                if remaining_quantity >= available_quantity:  
                                    remaining_quantity -= available_quantity  
                                    del self.open_buys[prod][price]  
                                else:  
                                    self.open_buys[prod][price] -= remaining_quantity  
                                    remaining_quantity = 0
                            else:
                                continue
                        if remaining_quantity > 0:
                            if trade.price in self.open_sells[prod]:
                                self.open_sells[prod][trade.price] += remaining_quantity
                            else:
                                self.open_sells[prod][trade.price] = remaining_quantity
                    self.recorded_time = trade.timestamp
            
            # sanity check: position
            if prod in state.position:
                if sum(self.open_buys[prod].values()) - sum(self.open_sells[prod].values()) != state.position[prod]:
                    print("Open positions incorrectly tracked!")
                # assert sum(self.open_buys[prod].values()) - sum(self.open_sells[prod].values()) == state.position[prod],\
                # "Open positions incorrectly tracked!"
            else:
                if self.open_buys or self.open_sells:
                    if sum(self.open_buys[prod].values()) - sum(self.open_sells[prod].values()) != 0:
                        print("Open positions incorrectly tracked!")
                    # assert sum(self.open_buys[prod].values()) - sum(self.open_sells[prod].values()) == 0, \
                    # "Open positions incorrectly tracked!"


    def order_am(self, state: TradingState):
        orders: list[Order] = []
        prod = "AMETHYSTS"
        # free parameters
        fairprice = 10000
        make_bid = fairprice - 2
        make_ask = fairprice + 2
        atol = 1
        param1 = 0.75
        # end of parameters

        # track long and short separately to prevent cancelling out
        current_short, current_long = 0, 0
        if prod in state.position:
            current_pos = state.position[prod]
            if current_pos > 0:
                current_long += current_pos
            else:
                current_short += current_pos

        pos_lim = self.POS_LIM[prod]
        order_depth = state.order_depths[prod]
        sellorders = sorted(list(order_depth.sell_orders.items()))
        buyorders = sorted(list(order_depth.buy_orders.items()), reverse=True)
        
        # market taking
        for sellorder in sellorders:
            ask, ask_amount = sellorder

            if current_long < pos_lim:
                if ask <= fairprice + atol:
                    mybuyvol = min(-ask_amount, pos_lim-current_long)
                    assert(mybuyvol >= 0), "Buy volume negative"
                    orders.append(Order(prod, ask, mybuyvol))
                    current_long += mybuyvol
                else:
                    # if price is higher than the fp, can still buy if it's lower than the current open sells
                    price_list = sorted(list(self.open_sells[prod].keys()))
                    for price in price_list:
                        if ask < price:
                            mybuyvol = min(ask_amount, self.open_sells[prod][price],
                                            pos_lim-current_long)
                            assert(mybuyvol >= 0), "Buy volume negative"
                            orders.append(Order(prod, ask, mybuyvol))
                            current_long += mybuyvol

        for buyorder in buyorders:
            bid, bid_amount = buyorder

            if current_short > -pos_lim:
                if bid >= fairprice - atol:
                    mysellvol = min(bid_amount, pos_lim+current_short)
                    mysellvol *= -1
                    assert(mysellvol <= 0), "Sell volume positive"
                    orders.append(Order(prod, bid, mysellvol))
                    current_short += mysellvol
                else:
                    price_list = sorted(list(self.open_buys[prod].keys()), reverse=True)
                    for price in price_list:
                        if bid > price:
                            mysellvol = min(bid_amount, self.open_buys[prod][price],
                                            pos_lim+current_short)
                            assert(mysellvol <= 0), "Sell volume positive"
                            orders.append(Order(prod, bid, mysellvol))
                            current_short += mysellvol

        # market making: fill the remaining orders up to position limit
        if current_long < pos_lim:
            qty1 = int((pos_lim - current_long) * param1)
            qty2 = pos_lim - current_long - qty1
            orders.append(Order(prod, make_bid, qty1))
            orders.append(Order(prod, make_bid-1, qty2))   # try to buy even lower
        if current_short > -pos_lim:
            qty1 = int((pos_lim + current_short) * param1)
            qty2 = pos_lim + current_short - qty1
            orders.append(Order(prod, make_ask, -qty1))
            orders.append(Order(prod, make_ask+1, -qty2))   # try to sell even higher

        return orders


    def order_st(self, state: TradingState):
        orders: list[Order] = []
        prod = "STARFRUIT"
        order_depth = state.order_depths[prod]
        # calculate fairprice based on market-making bots
        fairprice = (max(order_depth.sell_orders, key=order_depth.sell_orders.get) 
              + max(order_depth.buy_orders, key=order_depth.buy_orders.get)) / 2
        # self.kelp_hist.append(fairprice)

        # free parameters
        pos_lim = self.POS_LIM[prod]
        atol = 1
        param1 = 0.75
        make_bid = fairprice - 2
        make_ask = fairprice + 2
        # end of parameters

        # track long and short separately to prevent cancelling out
        current_short, current_long = 0, 0
        if prod in state.position:
            current_pos = state.position[prod]
            if current_pos > 0:
                current_long += current_pos
            else:
                current_short += current_pos
        
        sellorders = sorted(list(order_depth.sell_orders.items()))
        buyorders = sorted(list(order_depth.buy_orders.items()), reverse=True)
        
        # market taking
        for sellorder in sellorders:
            ask, ask_amount = sellorder

            if current_long < pos_lim:
                if ask <= fairprice + atol:
                    mybuyvol = min(-ask_amount, pos_lim-current_long)
                    assert(mybuyvol >= 0), "Buy volume negative"
                    orders.append(Order(prod, ask, mybuyvol))
                    current_long += mybuyvol
                else:
                    # close open positions: buy if it's lower than the current open sells
                    price_list = sorted(list(self.open_sells[prod].keys()))
                    for price in price_list:
                        if ask < price:
                            mybuyvol = min(ask_amount, self.open_sells[prod][price],
                                            pos_lim-current_long)
                            assert(mybuyvol >= 0), "Buy volume negative"
                            orders.append(Order(prod, ask, mybuyvol))
                            current_long += mybuyvol

        for buyorder in buyorders:
            bid, bid_amount = buyorder

            if current_short > -pos_lim:
                if bid >= fairprice - atol:
                    mysellvol = min(bid_amount, pos_lim+current_short)
                    mysellvol *= -1
                    assert(mysellvol <= 0), "Sell volume positive"
                    orders.append(Order(prod, bid, mysellvol))
                    current_short += mysellvol
                else:
                    price_list = sorted(list(self.open_buys[prod].keys()), reverse=True)
                    for price in price_list:
                        if bid > price:
                            mysellvol = min(bid_amount, self.open_buys[prod][price],
                                            pos_lim+current_short)
                            assert(mysellvol <= 0), "Sell volume positive"
                            orders.append(Order(prod, bid, mysellvol))
                            current_short += mysellvol

        # market making: fill the remaining orders up to position limit
        if current_long < pos_lim:
            qty1 = int((pos_lim - current_long) * param1)
            qty2 = pos_lim - current_long - qty1
            orders.append(Order(prod, make_bid, qty1))
            orders.append(Order(prod, make_bid-1, qty2))   # try to buy even lower
        if current_short > -pos_lim:
            qty1 = int((pos_lim + current_short) * param1)
            qty2 = pos_lim + current_short - qty1
            orders.append(Order(prod, make_ask, -qty1))
            orders.append(Order(prod, make_ask+1, -qty2))   # try to sell even higher

            # self.kelp_hist = self.kelp_hist[1:]

        return orders


    def run(self, state: TradingState):
        result = {}

        self.update_open_pos(state)
        result["AMETHYSTS"] = self.order_resin(state)
        result["STARFRUIT"] = self.order_kelp(state)

        traderData = "SAMPLE"
        
        conversions = 1
        return result, conversions, traderData
