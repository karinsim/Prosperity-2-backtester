# author: Karin Nakanishi
import numpy as np
from datamodel import OrderDepth, TradingState, Listing, Trade
from typing import Dict, List
from utils import aggregate_trades

np.random.seed(42)

Symbol = str

# Time = int
# Product = str
# Position = int
# UserId = str
# ObservationValue = int

PRODUCTS = ["STARFRUIT", "AMETHYSTS", "ORCHIDS", 
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


class Exchange:

    def __init__(self):
        self.products = PRODUCTS
        self.pos_limit = POS_LIM

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
                        timestamp=0,
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

            if len(list(sell_o.keys())) > 0 and len(list(buy_o.keys())) > 0:
                midprice = (min(list(sell_o.keys())) + max(list(buy_o.keys()))) / 2
            else:
                midprice = 0
            
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
                    if np.random.random() < p:
                        for price in outstanding_buy.keys():
                            if price > midprice:
                                qty = int(q*outstanding_buy[price]+0.5)
                                assert qty > 0, "Buy quantity negative!"
                                mytrades.append(Trade(
                                                    symbol=prod,
                                                    price=price,
                                                    quantity=qty,
                                                    buyer="SUBMISSION",
                                                    seller="",
                                                    timestamp=state_previous.timestamp))
                                position[prod] += qty
                            
                        for price in outstanding_sell.keys():
                            if price < midprice:
                                qty = int(q*outstanding_sell[price]-0.5)
                                assert qty < 0, "Sell quantity positive!"
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
                    if not np.isnan(listing_next[bprice].item()):
                        buy_next[listing_next[bprice].item()] = listing_next[bvol].item()
                for i in range(1, 4):
                    aprice = "ask_price_" + str(i)
                    avol = "ask_volume_" + str(i)
                    if not np.isnan(listing_next[aprice].item()):
                        sell_next[listing_next[aprice].item()] = -listing_next[avol].item()

            for key in sell_o.keys():
                if key > max(list(sell_next.keys())):
                    sell_next[key] = sell_o[key]
            for key in buy_o.keys():
                if key < min(list(buy_next.keys())):
                    buy_next[key] = buy_o[key]
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

        open_buy_all = {p:[] for p in self.products}
        open_sell_all = {p:[] for p in self.products}
        pnls = {p:[0.] for p in self.products}
        pnl_realised = {p:0 for p in self.products}

        for time in timestamps:
            price_now = prices.loc[prices["timestamp"]==time]
            # match previous listing with previous algo order
            state = self.match(time, price_now, state, algo_order, 
                               extra_bot_orders=extra_bot_orders, p=p, q=q) 
            self.state = state
            
            # calculate pnls
            for prod in self.products:
                latest_trades = state.own_trades[prod]

                if len(latest_trades) == 0:
                    pnls[prod].append(pnls[prod][-1])

                else:
                    open_buy = open_buy_all[prod]
                    open_sell = open_sell_all[prod]
                    pnl_unrealised = 0.
                    if len(price_now.loc[price_now["product"]==prod]["mid_price"]) == 0:
                        price_prev = prices.loc[prices["timestamp"]==time-dt]
                        current_price = price_prev.loc[price_prev["product"]==prod]["mid_price"].item()
                    else:
                        current_price = price_now.loc[price_now["product"]==prod]["mid_price"].item()

                    for trade in latest_trades:
                        if trade.buyer == "SUBMISSION":
                            open_buy.append({
                                "price": trade.price,
                                "quantity": trade.quantity,
                                "unrealised": current_price - trade.price
                            })
                        elif trade.seller == "SUBMISSION":
                            open_sell.append({
                                "price": trade.price,
                                "quantity": trade.quantity,
                                "unrealised": trade.price - current_price 
                            })
                    
                    # Realised PnL: we close the oldest positions first
                    while len(open_buy) != 0 and len(open_sell) != 0:
                        buy = open_buy[0]
                        sell = open_sell[0]
                        close_qty = min(buy["quantity"], sell["quantity"])
                        pnl_realised[prod] += (sell["price"] - buy["price"]) * close_qty
                        buy["quantity"] -= close_qty
                        sell["quantity"] -= close_qty

                        if buy["quantity"] == 0:
                            open_buy = open_buy[1:]
                        if sell["quantity"] == 0:
                            open_sell = open_sell[1:]
                    
                    # Unrealised pnl
                    if len(open_buy) == 0 and len(open_sell) != 0:
                        for sell in open_sell:
                            pnl_unrealised += sell["unrealised"] * sell["quantity"]
                    elif len(open_sell) == 0 and len(open_buy) != 0:
                        for buy in open_buy:
                            pnl_unrealised += buy["unrealised"] * buy["quantity"]    

                    pnls[prod].append(pnl_realised[prod] + pnl_unrealised)

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
            pnls[prod] = pnls[prod][1:]

        return pnls

