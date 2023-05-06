from portfolio.portfolio_utils import PortfolioUtils as portutil
from database.adatabase import ADatabase
import pickle
from datetime import datetime, timedelta
from modeler.modeler import Modeler as m
import pandas as pd
from factory.buy_factory import BuyFactory as buyfact
from factory.queue_factory import QueueFactory as queuefact
from factory.sell_factory import SellFactory as sellfact

class APortfolio(object):
    
    def __init__(self,strat_class,modeler_class,diversifier_class,positions,state):
        self.state = state
        self.strat_class = strat_class
        self.modeler_class = modeler_class
        self.diversifier_class = diversifier_class
        self.positions = self.diversifier_class.positions()
    def db_subscribe(self):
        self.db.connect()
    
    def db_unsubscribe(self):
        self.db.disconnect()

    def initialize_backtest_portfolio(self,parameters,initial,date):
        self.portfolio_state = portutil.portfolio_init(parameters,initial,date,self.positions)

    def initialize_portfolio(self,parameters,initial,date):
        portfolios = self.db.retrieve(f"portfolios")
        if portfolios.index.size < 1:
            self.portfolio_state = portutil.portfolio_init(parameters,initial,date,self.positions)
        else:
            portfolios["positions"] = [pickle.loads(x) for x in portfolios["positions"]]
            self.portfolio_state = portfolios[portfolios["date"]<=date].sort_values("date",ascending=False).head(self.positions).to_dict("records")[0]
    
    def pull_trades(self):
        trades = self.db.retrieve(f"trades")
        return trades
    
    def pull_portfolios(self):
        portfolios = self.db.retrieve(f"portfolios")
        return portfolios
    
    def drop_trades(self):
        trades = self.db.drop(f"trades")
        return trades
    
    def drop_portfolios(self):
        portfolios = self.db.drop(f"portfolios")
        return portfolios
    

    def pull_portfolio_max_date(self):
        date_range = self.db.retrieve_collection_date_range(f"portfolios")
        if date_range.index.size < 1:
            max_date = datetime(2022,1,1)
        else:
            max_date = date_range.iloc[-1].item() + timedelta(days=1)
        return max_date
    
    def pull_visualization_max_date(self):
        date_range = self.db.retrieve_collection_date_range(f"visualization")
        if date_range.index.size < 1:
            max_date = datetime(2022,1,1)
        else:
            max_date = date_range.iloc[-1].item() + timedelta(days=1)
        return max_date

    def pull_parameters(self):
        backtest_db = ADatabase(f"universal_{self.strat_class.name}_{self.modeler_class.name}_{self.diversifier_class.name}_backtest")
        backtest_db.connect()
        optimal_parameters = backtest_db.retrieve("optimal_parameters")
        backtest_db.disconnect()
        return optimal_parameters
    
    def pull_recs(self):
        self.db.connect()
        data = self.db.retrieve("recs")
        self.db.disconnect()
        return data

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
    
    def queue_clause(self,position_dictionary,todays_recs):
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

    def queue(self,date,position_dictionary,offerings):
        if self.queue_clause(position_dictionary,offerings) and self.strat_class.offering_clause(date):
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
                position_dictionary = self.entry(date,position_dictionary,daily_prices)  
                offerings =  self.strat_class.daily_rec(iterration_sim,date,current_tickers,parameter)
                try:
                    offerings = self.diversifier_class.diversify(offerings,position)
                    position_dictionary = self.queue(date,position_dictionary,offerings)
                    portfolio["positions"][position] = position_dictionary
                    current_tickers = portutil.current_ticker_list(portfolio)
                except Exception as e:
                    continue
        portfolio["positions"] = pickle.dumps(portfolio["positions"])
        self.db.store(f"portfolios",pd.DataFrame([portfolio]))
        portfolio["positions"] = pickle.loads(portfolio["positions"])
        self.portfolio_state = portfolio