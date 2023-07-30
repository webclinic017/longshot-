from database.market import Market
from database.sec import SEC
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from returns.products import Products
from parameters.parameters import Parameters as params

class Fund(object):

    def __init__(self,portfolios,backtest_start_date,backtest_end_date,current_start_date):
        self.market = Market()
        self.sec = SEC()
        self.backtest_start_date = backtest_start_date
        self.backtest_end_date = backtest_end_date
        self.current_start_date = current_start_date
        self.portfolios = portfolios

    def initialize_portfolios(self):
        for portfolio in self.portfolios:
            portfolio.initialize(self.backtest_start_date,self.backtest_end_date,self.current_start_date)
    
    def create_training_sets(self):
        for portfolio in self.portfolios:
            portfolio.create_training_sets()
    
    def initialize_backtesters(self):
        for portfolio in self.portfolios:
            portfolio.initialize_backtester(self.backtest_start_date,self.backtest_end_date)
    
    def pull_recommendation(self):
        recs = []
        for portfolio in self.portfolios:
            portfolio.db.cloud_connect()
            rec = portfolio.db.retrieve("recs")
            portfolio.db.disconnect()
            rec["portfolio"] = portfolio.name
            recs.append(rec)
        return recs
    
    def pull_orders(self):
        recs = []
        for portfolio in self.portfolios:
            rec = portfolio.pull_orders()
            rec["portfolio"] = portfolio.name
            recs.append(rec)
        return pd.concat(recs)
    
    def run_recommendation(self):
        self.market.cloud_connect()
        sp500 = self.market.retrieve("sp500").rename(columns={"Symbol":"ticker"})
        bench = Products.spy_bench(self.market.retrieve("spy"))
        self.market.disconnect()
        
        for portfolio in self.portfolios:
            try:
                self.market.cloud_connect()
                returns = portfolio.create_returns(self.market,bench,True)
                self.market.disconnect()

                print(returns.tail())
                portfolio.pricer_class.db.cloud_connect()
                sim = portfolio.pricer_class.db.retrieve("predictions").drop("adjclose",axis=1,errors="ignore")
                portfolio.pricer_class.db.disconnect()
                
                merged = portfolio.merge_sim_returns(sim,returns)
                naming = portfolio.pricer_class.time_horizon_class.naming_convention
                merged = merged.sort_values(["year",naming,"day"]).dropna()
                
                parameter = portfolio.parameter
                rec = portfolio.run_backtest(self.market,merged.copy(),parameter,True)
                if rec.index.size > 0:
                    trades = rec.merge(sp500[["ticker","Security","GICS Sector"]],on="ticker")
                    final = trades[["year",naming,"ticker","Security","GICS Sector","position",f"{naming}ly_delta",f"{naming}ly_delta_sign"]]
                    portfolio.db.cloud_connect()
                    portfolio.db.drop("recs")
                    portfolio.db.store("recs",final)
                    portfolio.db.disconnect()
            except Exception as e:
                portfolio.db.cloud_connect()
                portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"recommendations","error":str(e)}]))
                portfolio.db.disconnect()
    
    def run_backtest(self):
        self.market.connect()
        sp500 = self.market.retrieve("sp500").rename(columns={"Symbol":"ticker"})
        bench = Products.spy_bench(self.market.retrieve("spy"))
        self.market.disconnect()
        parameters = params.parameters()
        
        for portfolio in tqdm(self.portfolios):
            try:
                self.market.connect()
                returns = portfolio.create_returns(self.market,bench,False)
                self.market.disconnect()
                
                portfolio.pricer_class.db.connect()
                sim = portfolio.pricer_class.db.retrieve("sim")
                portfolio.pricer_class.db.disconnect()
                
                merged = portfolio.merge_sim_returns(sim,returns)
                merged = merged.sort_values(["year","week","day"]).dropna()
                
                portfolio.db.connect()
                for parameter in parameters:
                    trades = portfolio.db.query("trades",parameter)
                    if trades.index.size < 1:         
                        trades = portfolio.run_backtest(self.market,merged,parameter,False)
                        portfolio.db.store("trades",trades)
                    else:
                        continue
                portfolio.db.disconnect()
                for key in parameter.keys():
                    try:
                        portfolio.db.connect()
                        portfolio.db.create_index("trades",key)
                        portfolio.db.disconnect()
                    except Exception as e:
                        print(str(e),key)
            except Exception as e:
                print(str(e))
    
    def reset(self):
        for portfolio in self.portfolios:
            portfolio.reset()
