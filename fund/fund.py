from database.market import Market
from database.sec import SEC
from tqdm import tqdm
import pandas as pd
from returns.products import Products
from datetime import datetime

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
    
    def create_historical_models(self):
        for portfolio in self.portfolios:
            portfolio.create_historical_models()
    
    def initialize_backtesters(self):
        for portfolio in self.portfolios:
            portfolio.initialize_backtester(self.backtest_start_date,self.backtest_end_date)
    
    def run_recommendation(self):
        self.market.cloud_connect()
        sp500 = self.market.retrieve("sp500").rename(columns={"Symbol":"ticker"})
        tyields = Products.tyields(self.market.retrieve("tyields"))
        bench = Products.spy_bench(self.market.retrieve("spy"))
        self.market.disconnect()
        for portfolio in self.portfolios:
            try:
                parameter = portfolio.parameter
                self.market.cloud_connect()
                returns = portfolio.create_returns(self.market,bench,True)
                self.market.disconnect()
                portfolio.pricer_class.db.cloud_connect()
                sim = portfolio.pricer_class.db.retrieve("predictions").drop("adjclose",axis=1,errors="ignore")
                portfolio.pricer_class.db.disconnect()
                merged = portfolio.merge_sim_returns(sim,returns)
                merged = merged.sort_values(["year","week","day"]).dropna()
                rec = portfolio.run_recommendation(merged.copy(),tyields,parameter)
                trades = rec.merge(sp500[["ticker","Security","GICS Sector"]],on="ticker")
                final = trades[["year","week","ticker","Security","GICS Sector","position","weekly_delta","weekly_delta_sign"]]
                if final.index.size > 0:
                    portfolio.db.cloud_connect()
                    portfolio.db.drop("recs")
                    portfolio.db.store("recs",final)
                    portfolio.db.disconnect()

            except Exception as e:
                portfolio.db.cloud_connect()
                portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"recommendations","error":str(e)}]))
                portfolio.db.disconnect()
    
    def run_backtest_qa(self,parameter):
        self.market.connect()
        tyields = Products.tyields(self.market.retrieve("tyields"))
        bench = Products.spy_bench(self.market.retrieve("spy"))
        self.market.disconnect()
        for portfolio in tqdm(self.portfolios):
            self.market.connect()
            returns = portfolio.create_returns(self.market,bench,False)
            self.market.disconnect()
            portfolio.pricer_class.db.cloud_connect()
            sim = portfolio.pricer_class.db.retrieve("predictions").drop("adjclose",axis=1,errors="ignore")
            portfolio.pricer_class.db.disconnect()
            merged = portfolio.merge_sim_returns(sim,returns)
            merged = merged.sort_values(["year","week","day"]).dropna()
            final = portfolio.run_backtest_qa(merged,tyields,parameter)
            if final.index.size > 0:
                portfolio.db.cloud_connect()
                portfolio.db.drop("recs_qa")
                portfolio.db.store("recs_qa",final)
                portfolio.db.disconnect()
    
    def run_backtest(self):
        self.market.connect()
        tyields = Products.tyields(self.market.retrieve("tyields"))
        bench = Products.spy_bench(self.market.retrieve("spy"))
        self.market.disconnect()
        for portfolio in tqdm(self.portfolios):
            self.market.connect()
            returns = portfolio.create_returns(self.market,bench,False)
            self.market.disconnect()
            portfolio.pricer_class.db.connect()
            sim = portfolio.pricer_class.db.retrieve("sim")
            portfolio.pricer_class.db.disconnect()
            merged = portfolio.merge_sim_returns(sim,returns)
            merged = merged.sort_values(["year","week","day"]).dropna()
            portfolio.run_backtest(merged,tyields)
    
    def reset(self):
        for portfolio in self.portfolios:
            portfolio.reset()
