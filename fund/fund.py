from database.market import Market
from database.sec import SEC
# from strategy.strategy_factory import StratFactory as strat_fact


class Fund(object):

    def __init__(self,portfolios,model_start_date,model_end_date,current_start_date):
        self.market = Market()
        self.sec = SEC()
        self.model_start_date = model_start_date
        self.model_end_date = model_end_date
        self.current_start_date = current_start_date
        self.portfolios = portfolios

    def initialize_portfolios(self):
        for portfolio in self.portfolios:
            portfolio.initialize(self.model_start_date,self.model_end_date,self.current_start_date)
    
    def create_training_sets(self):
        for portfolio in self.portfolios:
            portfolio.create_training_sets()
    
    def create_historical_models(self):
        for portfolio in self.portfolios:
            portfolio.create_historical_models()
    
    def create_historical_simulations(self):
        for portfolio in self.portfolios:
            portfolio.create_historical_simulations()
    
    def run_historical_backtest(self):
        for portfolio in self.portfolios:
            portfolio.run_historical_backtest()
