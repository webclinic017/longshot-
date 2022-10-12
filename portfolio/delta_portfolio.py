import pandas as pd
from portfolio.portfolio_utils import PortfolioUtils as portutil
from factory.buy_factory import BuyFactory as buyfact
from factory.queue_factory import QueueFactory as queuefact
from factory.sell_factory import SellFactory as sellfact
import pickle
from datetime import datetime, timedelta
from portfolio.aportfolio import APortfolio

class DeltaPortfolio(APortfolio):
    
    def __init__(self,strat_class,positions,state):
        super().__init__(strat_class,positions,state)
    
    def daily_rec(self,iterration_sim,date,current_tickers,signal):
        todays_recs = iterration_sim[(iterration_sim["date"]==date) & (iterration_sim["delta"] >= signal) & (~iterration_sim["ticker"].isin(current_tickers))]
        todays_recs.sort_values("delta",ascending=False,inplace=True)
        return todays_recs
    
    def exit_clause(self,position_dictionary,req):
        if len(position_dictionary["asset"].keys()) > 0:
            asset_dictionary = position_dictionary["asset"]
            current_price = asset_dictionary["adjclose"]
            buy_price = asset_dictionary["buy_price"]
            current_gain = (current_price - buy_price) / buy_price
            return current_gain >= req
        else:
            return False
    
    def swap_clause(self,offerings,position_dictionary,signal,position):
        asset_dictionary = position_dictionary["asset"]
        if len(asset_dictionary.keys()) > 0 and offerings.index.size > position:
            offering = offerings.iloc[position]
            current_price = asset_dictionary["adjclose"]
            buy_price = asset_dictionary["buy_price"]
            current_gain = (current_price - buy_price) / buy_price
            return (current_gain > 0) and (offering["delta"] > (asset_dictionary["projected_delta"] - current_gain)) and (offering["delta"] >= signal)
        else:
            return False
    
    def queue_clause(self,position_dictionary,position,todays_recs):
        return (len(position_dictionary["asset"].keys()) == 0) and (todays_recs.index.size > 0)
    
    def entry_clause(self,date,position_dictionary,prices):
        if len(position_dictionary["queue"].keys())>0 and len(position_dictionary["asset"].keys()) == 0:
            purchase_dictionary = position_dictionary["queue"]
            entry_price = purchase_dictionary["adjclose"]
            ticker = purchase_dictionary["ticker"]
            price_data = prices[(prices["ticker"]==ticker) & (prices["date"]==date)]
            if price_data.index.size > 0:
                high = price_data["high"].iloc[0].item()
                low = price_data["low"].iloc[0].item()
                return entry_price <= high and entry_price >= low
        return False
    
    def exit(self,date,position_dictionary,parameter,position):
        if self.exit_clause(position_dictionary,parameter["req"]) or self.strat_class.exit_clause(date,position_dictionary):
            asset_dictionary = position_dictionary["asset"]
            trade = sellfact.sell_record(asset_dictionary,date,position,parameter)
            trade["strategy"] = self.strat_class.name
            position_dictionary = portutil.exit(position_dictionary)
        else:
            trade = {}
        return {"dictionary":position_dictionary,"trade":trade}

    def swap(self,date,position_dictionary,offerings,parameter,position):
        if self.swap_clause(offerings,position_dictionary,parameter["signal"],position) and self.strat_class.offering_clause(date):    
            asset_dictionary = position_dictionary["asset"]
            trade = sellfact.sell_record(asset_dictionary,date,position,parameter)
            position_dictionary = portutil.exit(position_dictionary)
            purchase_dictionary = queuefact.queue_record(position_dictionary,offerings.iloc[position],swap=True)
            position_dictionary["queue"] = purchase_dictionary
        else:
            trade = {}
        return {"dictionary":position_dictionary,"trade":trade}

    def entry(self,date,position_dictionary,daily_prices):
        if self.entry_clause(date,position_dictionary,daily_prices):
            purchase_dictionary = buyfact.buy_record(date,position_dictionary["queue"])
            position_dictionary = portutil.entry(position_dictionary,purchase_dictionary)
        else:
            position_dictionary["queue"] = {}
        return position_dictionary

    def queue(self,date,position_dictionary,offerings,position):
        if self.queue_clause(position_dictionary,position,offerings) and self.strat_class.offering_clause(date):
            purchase_dictionary = queuefact.queue_record(position_dictionary,offerings.iloc[0],swap=False)
            position_dictionary["queue"] = purchase_dictionary
        return position_dictionary
    
    def daily_iterration(self,iterration_sim,date,prices,parameter):
        daily_prices = prices[prices["date"]==date]
        portfolio = self.portfolio_state
        portfolio["date"] = date
        current_tickers = portutil.current_ticker_list(portfolio)
        if daily_prices.index.size > 0:
            portfolio = portutil.asset_updates(portfolio,daily_prices)
            for position in range(self.positions):
                current_tickers = portutil.current_ticker_list(portfolio)
                position_dictionary = portfolio["positions"][position]       
                exit_dictionary = self.exit(date,position_dictionary,parameter,position)
                position_dictionary = exit_dictionary["dictionary"]
                trade = exit_dictionary["trade"]
                if len(trade.keys()) > 0:
                    self.db.store(f"trades",pd.DataFrame([trade]))
                portfolio["positions"][position] = position_dictionary
                current_tickers = portutil.current_ticker_list(portfolio)
                if parameter["oc"]:
                    offerings =  self.daily_rec(iterration_sim,date,current_tickers,parameter["signal"])
                    exit_dictionary = self.swap(date,position_dictionary,offerings,parameter,position)
                    position_dictionary = exit_dictionary["dictionary"]
                    trade = exit_dictionary["trade"]
                    if len(trade.keys()) > 0:
                        self.db.store(f"trades",pd.DataFrame([trade]))
                    portfolio["positions"][position] = position_dictionary
                    current_tickers = portutil.current_ticker_list(portfolio)
                position_dictionary = self.entry(date,position_dictionary,daily_prices)  
                offerings =  self.daily_rec(iterration_sim,date,current_tickers,parameter["signal"])
                position_dictionary = self.queue(date,position_dictionary,offerings,position)
                portfolio["positions"][position] = position_dictionary
                current_tickers = portutil.current_ticker_list(portfolio)
        portfolio["positions"] = pickle.dumps(portfolio["positions"])
        self.db.store(f"portfolios",pd.DataFrame([portfolio]))
        portfolio["positions"] = pickle.loads(portfolio["positions"])
        self.portfolio_state = portfolio
    