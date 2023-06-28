from database.market import Market
from database.sec import SEC
from tqdm import tqdm
import pandas as pd
from returns.products import Products
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
    
    # def run_recommendation(self):
    #     recs = []
    #     for portfolio in tqdm(self.portfolios):
    #         sim = portfolio.create_simulation()
    #         returns = portfolio.create_risk_returns()
    #         sim_returns = portfolio.merge_sim_returns(sim,returns)
    #         rec = pd.concat(portfolio.recommendation(sim_returns))
    #         rec["portfolio"] = portfolio.name
    #         recs.append(rec)
    #     return recs
    
    def run_backtest(self,market):
        for portfolio in tqdm(self.portfolios):
            market.connect()
            tyields = Products.tyields(market.retrieve("tyields"))
            bench = Products.spy_bench(market.retrieve("spy"))
            returns = portfolio.create_returns(market,bench,False)
            market.disconnect()
            sim = portfolio.create_simulation()
            merged = portfolio.merge_sim_returns(sim,returns)
            portfolio.run_backtest(merged,tyields)
    
    def reset(self):
        for portfolio in self.portfolios:
            portfolio.reset()
