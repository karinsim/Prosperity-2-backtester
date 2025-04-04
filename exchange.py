# author: Karin Nakanishi
import numpy as np
from datamodel import OrderDepth, TradingState, Listing, Trade
from typing import Dict, List
from utils import aggregate_trades

np.random.seed(42)

Symbol = str

# PRODUCTS = ["STARFRUIT", "AMETHYSTS", "ORCHIDS", 
#             "CHOCOLATE", "STRAWBERRIES", "ROSES", 
#             "GIFT_BASKET", "COCONUT", "COCONUT_COUPON"]

PRODUCTS = ["RAINFOREST_RESIN", "KELP"]

POS_LIM = {PRODUCTS[0]: 50,
        PRODUCTS[1]: 50,
        "ORCHIDS": 100,
        "CHOCOLATE": 250,
        "STRAWBERRIES": 350,
        "ROSES": 60,
        "GIFT_BASKET": 60,
        "COCONUT": 300,
        "COCONUT_COUPON": 600}


class Exchange:

    def __init__(self):
        self.products = PRODUCTS
        self.pos_limit = POS_LIM

        self.open_buy_all = {p:[] for p in self.products}
        self.open_sell_all = {p:[] for p in self.products}
        self.pnl_realised = {p:0 for p in self.products}
        self.pnl_total = {p:[0.] for p in self.products}

        listings = {prod:Listing(
                        symbol=prod, product=prod, denomination="SEASHELLS")
                    for prod in self.products}
        order_depth_start = {p:OrderDepth(buy_orders={}, sell_orders={}) 
                            for p in self.products}
        algo_order_start = {p:[] for p in self.products}
        trades_start = {p:[] for p in self.products}
        self.trade_hist = {p:[] for p in self.products}
        position = {p:0 for p in self.products}

        self.state_start = TradingState(
                        traderData="local",
                        timestamp=-1,
                        listings=listings,
                        order_depths=order_depth_start,
                        own_trades=trades_start,
                        market_trades=trades_start,
                        position=position)
        self.algo_order_start = algo_order_start

        # maybe delete
        self.state = self.state_start


    def match(
            self, time, price_next, state_previous, myorders, 
            extra_bot_orders="always", p=1., q=1.
            ):

        listings: Dict[Symbol, Listing] = {}
        order_depths: Dict[Symbol, OrderDepth] = {}
        own_trades: Dict[Symbol, List[Trade]] = {}
        market_trades: Dict[Symbol, List[Trade]] = {}

        assert extra_bot_orders in ["always", "never", "probabilistic"], \
            "Unrecognised parameter choice for extra_bot_orders"
        
        position = state_previous.position

        for prod in self.products:
            
            listings[prod] = Listing(
                symbol=prod, product=prod, denomination="SEASHELLS")
            mytrades = []

            # market bid and ask orders
            # take directly from previous trading state - this is what the Trader saw to send his orders
            sell_o = state_previous.order_depths[prod].sell_orders
            buy_o = state_previous.order_depths[prod].buy_orders
            
            # process orders placed by self (the algorithm)
            mybuyorders, mysellorders = {}, {}
            
            if prod in myorders.keys():
                myorders_prod = myorders[prod]

                # check position limit is observed -- otherwise cancel all orders
                nobuy, nosell = False, False
                quantities = np.array([order.quantity for order in myorders_prod])

                if quantities[np.where(quantities > 0)[0]].sum() + position[prod] > self.pos_limit[prod]:
                    nobuy = True
                    print("Position limit exceeded; cancelling all BUY orders at time", time)
                if quantities[np.where(quantities < 0)[0]].sum() + position[prod] < -self.pos_limit[prod]:
                    nosell = True
                    print("Position limit exceeded; cancelling all SELL orders at time", time)

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
                
                mybuyorders = dict(sorted(mybuyorders.items()))
                mysellorders = dict(sorted(mysellorders.items(), reverse=True))

                # match orders
                for i in range(len(mybuyorders)):
                    myp, myv = list(mybuyorders.items())[i]
                    marketsellprice = list(sell_o.keys())
                    matchedprice = sorted([p for p in marketsellprice if myp >= p])

                    while myv > 0 and len(matchedprice) > 0:
                        trade_vol = min(myv, -sell_o[matchedprice[0]])
                        assert trade_vol > 0, "Trade volume negative"
                        
                        # print("BUY VOL", trade_vol)

                        mytrades.append(Trade(
                                            symbol=prod,
                                            price=matchedprice[0],
                                            quantity=trade_vol,
                                            buyer="SUBMISSION",
                                            seller="",
                                            timestamp=state_previous.timestamp))
                        
                        myv -= trade_vol
                        mybuyorders[myp] -= trade_vol
                        sell_o[matchedprice[0]] += trade_vol
                        position[prod] += trade_vol

                        if sell_o[matchedprice[0]] == 0:
                            del(sell_o[matchedprice[0]])
                            matchedprice.pop(0)

                keys = list(mybuyorders.keys())
                for key in keys:
                    if mybuyorders[key] == 0:
                        del(mybuyorders[key])

                outstanding_buy = mybuyorders
            
                for i in range(len(mysellorders)):
                    currrent_sell = list(mysellorders.items())[i]

                    myp, myv = currrent_sell

                    marketbuyprice = list(buy_o.keys())
                    matchedprice = sorted([p for p in marketbuyprice if p >= myp], reverse=True)

                    while myv < 0 and len(matchedprice) > 0:
                        trade_vol = min(-myv, buy_o[matchedprice[0]])
                        assert trade_vol > 0, "Trade volume negative"

                        # print("SELL VOL", trade_vol)

                        mytrades.append(Trade(
                                            symbol=prod,
                                            price=matchedprice[0],
                                            quantity=trade_vol,
                                            buyer="",
                                            seller="SUBMISSION",
                                            timestamp=state_previous.timestamp))

                        myv += trade_vol
                        mysellorders[myp] += trade_vol
                        buy_o[matchedprice[0]] -= trade_vol
                        position[prod] -= trade_vol

                        if buy_o[matchedprice[0]] == 0:
                            del(buy_o[matchedprice[0]])
                            matchedprice.pop(0)

                keys = list(mysellorders.keys())
                for key in keys:
                    if mysellorders[key] == 0:
                        del(mysellorders[key])

                outstanding_sell = mysellorders

                # deal with the outstanding algo orders
                # compare against current orderbook sell_o and buy_o
                if extra_bot_orders == "always":
                    for price in outstanding_buy.keys():
                        mytrades.append(Trade(
                                            symbol=prod,
                                            price=price,
                                            quantity=outstanding_buy[price],
                                            buyer="SUBMISSION",
                                            seller="",
                                            timestamp=state_previous.timestamp))
                        position[prod] += outstanding_buy[price]
                    
                    for price in outstanding_sell.keys():
                        mytrades.append(Trade(
                                            symbol=prod,
                                            price=price,
                                            quantity=-outstanding_sell[price],
                                            buyer="",
                                            seller="SUBMISSION",
                                            timestamp=state_previous.timestamp))
                        position[prod] += outstanding_sell[price]

                elif extra_bot_orders == "probabilistic":
                    # the probability of execution should scale with the rank of algo bid vs other bids in orderbook
                    # penalise the probability of execution proportionally to rank
                    for price in outstanding_buy.keys():
                        market_buyo = list(buy_o.keys())
                        rank = len([x for x in market_buyo if x > price])
                        if rank == 0:
                            if np.random.random() < p:
                                qty = round(q*outstanding_buy[price])
                                assert qty >= 0, "Buy quantity negative!"
                                if qty > 0:
                                    mytrades.append(Trade(
                                                        symbol=prod,
                                                        price=price,
                                                        quantity=qty,
                                                        buyer="SUBMISSION",
                                                        seller="",
                                                        timestamp=state_previous.timestamp))
                                    position[prod] += qty
                        else:
                            penalty = rank / len(market_buyo)
                            if np.random.random() < p - penalty:
                                qty = round(q*outstanding_buy[price])
                                assert qty >= 0, "Buy quantity negative!"
                                if qty > 0:
                                    mytrades.append(Trade(
                                                        symbol=prod,
                                                        price=price,
                                                        quantity=qty,
                                                        buyer="SUBMISSION",
                                                        seller="",
                                                        timestamp=state_previous.timestamp))
                                    position[prod] += qty
                        
                        for price in outstanding_sell.keys():
                            market_sello = list(sell_o.keys())
                            rank = len([x for x in market_sello if x < price])
                            if rank == 0:
                                if np.random.random() < p:
                                    qty = round(q*outstanding_sell[price])
                                    assert qty <= 0, "Sell quantity positive!"
                                    if qty < 0:
                                        mytrades.append(Trade(
                                                            symbol=prod,
                                                            price=price,
                                                            quantity=-qty,
                                                            buyer="",
                                                            seller="SUBMISSION",
                                                            timestamp=state_previous.timestamp))
                                        position[prod] += qty
                            else:
                                penalty = rank / len(market_sello)
                                if np.random.random() < p - penalty:
                                    qty = round(q*outstanding_sell[price])
                                    assert qty <= 0, "Sell quantity positive!"
                                    if qty < 0:
                                        mytrades.append(Trade(
                                                            symbol=prod,
                                                            price=price,
                                                            quantity=-qty,
                                                            buyer="",
                                                            seller="SUBMISSION",
                                                            timestamp=state_previous.timestamp))
                                        position[prod] += qty
                
                mytrades = aggregate_trades(mytrades)

            own_trades[prod] = mytrades
                
            # add next market orders and current outstanding market orders (quotes) conditionally to be included in TradingState
            sell_next = {}
            buy_next = {}
            listing_next = price_next.loc[price_next["product"]==prod]

            if len(listing_next["product"]) > 0:
                for i in range(1, 4):
                    bprice = "bid_price_" + str(i)
                    bvol = "bid_volume_" + str(i)
                    if not np.isnan(listing_next[bprice].item()) and listing_next[bvol].item() > 0:
                        buy_next[listing_next[bprice].item()] = listing_next[bvol].item()
                for i in range(1, 4):
                    aprice = "ask_price_" + str(i)
                    avol = "ask_volume_" + str(i)
                    if not np.isnan(listing_next[aprice].item()) and listing_next[avol].item() > 0:
                        sell_next[listing_next[aprice].item()] = -listing_next[avol].item()

            for key in sell_o.keys():
                if key > max(list(sell_next.keys())):
                    sell_next[key] = min(sell_o[key], -10)   # to not mess with market-making bots orders
            for key in buy_o.keys():
                if key < min(list(buy_next.keys())):
                    buy_next[key] = max(buy_o[key], 10)     # to not mess with market-making bots orders
            order_depths[prod] = OrderDepth(
                    buy_orders=buy_next, sell_orders=sell_next)
        
        # maybe delete
        self.state = TradingState(
                    traderData="local",
                    timestamp=time,
                    listings=listings,
                    order_depths=order_depths,
                    own_trades=own_trades,
                    market_trades=market_trades,
                    position=position)

        return TradingState(
                    traderData="local",
                    timestamp=time,
                    listings=listings,
                    order_depths=order_depths,
                    own_trades=own_trades,
                    market_trades=market_trades,
                    position=position)


    def iterate(self, timestamps, prices, trader, 
                extra_bot_orders="always", p=1., q=1., 
                verbose=0, logging=False,
                testing=(False, [], [])):

        state = self.state_start
        algo_order = self.algo_order_start
        
        dt = list(timestamps)[1] - list(timestamps)[0]
        

        for time in timestamps:

            # print("TIME: ", time)

            price_now = prices.loc[prices["timestamp"]==time]
            # match previous listing with previous algo order
            state = self.match(time, price_now, state, algo_order, 
                               extra_bot_orders=extra_bot_orders, p=p, q=q) 
            self.state = state
            
            # calculate pnls
            for prod in self.products:
                pnl_unrealised = 0.
                if len(price_now.loc[price_now["product"]==prod]["mid_price"]) == 0:
                    # if current price unavailable, use previous price
                    price_prev = prices.loc[prices["timestamp"]==time-dt]
                    current_price = price_prev.loc[price_prev["product"]==prod]["mid_price"].item()
                else:
                    current_price = price_now.loc[price_now["product"]==prod]["mid_price"].item()

                latest_trades = state.own_trades[prod]
                if len(latest_trades) == 0:
                    # print("prod", prod , "time", time, "open buy", self.open_buy_all, "sell", self.open_sell_all)
                    if len(self.open_buy_all[prod]) == 0 and len(self.open_sell_all[prod]) == 0:    # no open position
                        self.pnl_total[prod].append(self.pnl_total[prod][-1])
                    else:
                        # Unrealised pnl
                        if len(self.open_buy_all[prod]) == 0 and len(self.open_sell_all[prod]) != 0:
                            open_sell = self.open_sell_all[prod]
                            for sell in open_sell:
                                pnl_unrealised += (sell["price"]-current_price) * sell["quantity"]
                        elif len(self.open_sell_all[prod]) == 0 and len(self.open_buy_all[prod]) != 0:
                            open_buy = self.open_buy_all[prod]
                            for buy in open_buy:
                                pnl_unrealised += (current_price-buy["price"]) * buy["quantity"]    

                        self.pnl_total[prod].append(self.pnl_realised[prod] + pnl_unrealised)
            
                else:
                    # print(time, "trades: ", latest_trades)
                    for trade in latest_trades:
                        if trade.buyer == "SUBMISSION":
                            self.open_buy_all[prod].append({
                                "price": trade.price,
                                "quantity": int(trade.quantity),
                            })
                        elif trade.seller == "SUBMISSION":
                            self.open_sell_all[prod].append({
                                "price": trade.price,
                                "quantity": int(trade.quantity),
                            })
                    
                    # Realised PnL: we close the oldest positions first
                    while len(self.open_buy_all[prod]) != 0 and len(self.open_sell_all[prod]) != 0:
                        buy = self.open_buy_all[prod][0]
                        sell = self.open_sell_all[prod][0]
                        close_qty = int(min(buy["quantity"], sell["quantity"]))
                        self.pnl_realised[prod] += (sell["price"] - buy["price"]) * close_qty
                        self.open_buy_all[prod][0]["quantity"] -= close_qty
                        self.open_sell_all[prod][0]["quantity"] -= close_qty
                        if close_qty == buy["quantity"]:
                            self.open_buy_all[prod].pop(0)
                        if close_qty == sell["quantity"]:
                            self.open_sell_all[prod].pop(0)
                    
                    # print("prod", prod , "time", time, "open buy", self.open_buy_all, "sell", self.open_sell_all)
                    
                    # Unrealised pnl
                    if len(self.open_buy_all[prod]) == 0 and len(self.open_sell_all[prod]) != 0:
                        open_sell = self.open_sell_all[prod]
                        for sell in open_sell:
                            pnl_unrealised += (sell["price"]-current_price) * sell["quantity"]
                    elif len(self.open_sell_all[prod]) == 0 and len(self.open_buy_all[prod]) != 0:
                        open_buy = self.open_buy_all[prod]
                        for buy in open_buy:
                            pnl_unrealised += (current_price-buy["price"]) * buy["quantity"]    

                    self.pnl_total[prod].append(self.pnl_realised[prod] + pnl_unrealised)

            # run trading algorithm
            if testing[0] == True:
                algo_order = trader.run_test(state, testing[1], testing[2])
            else:
                algo_order = trader.run(state)

            if verbose >= 1:
                print("-----------------------------------------------")
                print("timestamp: ", time)
                print("position: ", state.position)
                if verbose >= 2:
                    print("own trades: ", state.own_trades)
                    if verbose >= 3:
                        for prod in self.products:
                            print("order depths, sell (", prod, "): ", 
                                state.order_depths[prod].sell_orders, 
                                ", order depths, buy (", prod, "): ",  
                                state.order_depths[prod].buy_orders)
            
            # log trade history if requested
            if logging:
                for prod in self.products:
                    latest_trades = state.own_trades[prod]
                    if len(latest_trades) > 0:
                        self.trade_hist[prod] += latest_trades
        
        for prod in self.products:
            self.pnl_total[prod] = self.pnl_total[prod][1:]

        return self.pnl_total

