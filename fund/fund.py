from database.market import Market
from database.sec import SEC
from backtester.abacktester import ABacktester


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
    
    def initialize_historical_backtesters(self):
        for portfolio in self.portfolios:
            portfolio.initialize_historical_backtester(self.backtest_start_date,self.backtest_end_date)
    
    def initialize_backtesters(self):
        for portfolio in self.portfolios:
            portfolio.initialize_backtester(self.backtest_start_date,self.backtest_end_date)

    def run_historical_backtest(self):
        for portfolio in self.portfolios:
            sim = portfolio.create_simulation()
            returns = portfolio.create_returns()
            sim_returns = portfolio.merge_sim_returns(sim,returns)
            portfolio.run_backtest(sim_returns)
    
    def run_backtest(self):
        for portfolio in self.portfolios:
            sim = portfolio.create_current_simulation()
            returns = portfolio.create_returns()
            sim_returns = portfolio.merge_sim_returns(sim,returns)
            portfolio.run_backtest(sim_returns)
    
    def reset(self):
        for portfolio in self.portfolios:
            portfolio.reset()
