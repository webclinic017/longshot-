from database.adatabase import ADatabase
from database.market import Market
from database.sec import SEC
from portfolio.delta_portfolio import DeltaPortfolio
from strats.strat_fact import StratFactory as stratfact
from simulator.simulator import Simulator
from processor.processor import Processor as p
from visualizer.visualizer import Visualizer
from datetime import datetime
from backtester.backtester import Backtester
class Fund(object):

    def __init__(self,name,state):
        self.name = name
        self.db = ADatabase(name)
        self.state = state
        self.market = Market()
        self.sec = SEC()
        self.start_year = 2016
        self.end_year = 2022
        self.start_date = datetime(2016,1,1)
        self.end_date = datetime(2022,1,1)
        self.initial = 100000

    def pull_portfolios(self):
        self.db.connect()
        portfolios = self.db.retrieve("portfolios")
        self.db.disconnect()
        self.portfolios =  portfolios.to_dict("records")
    
    def initialize_portfolios(self):
        for portfolio in self.portfolios:
            portfolio_name = portfolio["name"]
            positions = portfolio["positions"]
            strat_class = stratfact.build_strat(portfolio_name)
            port_class = DeltaPortfolio(strat_class,positions,self.state)
            portfolio["portfolio_class"] = port_class
    
    def transform(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            portfolio_class.transform(self.market,self.sec)
    
    def model(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            portfolio_class.model(self.start_year,self.end_year)
    
    def backtest(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            strat_class = portfolio_class.strat_class
            price_db = strat_class.price_db
            self.market.connect()
            prices = self.market.retrieve(price_db)
            self.market.disconnect()
            prices = p.column_date_processing(prices)
            portfolio_class.db_subscribe()
            simulation = portfolio_class.db.retrieve("sim")
            portfolio_class.db_unsubscribe()
            sim = strat_class.create_sim(prices.copy(),simulation)
            backtester = Backtester(self.start_date,self.end_date,portfolio_class,self.initial)
            backtester.parameters_init()
            backtester.backtest(sim.copy(),prices)
    
    def recommend_transform(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            portfolio_class.recommend_transform(self.market,self.sec)
    
    def recommend_model(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            year = datetime.now().year
            portfolio_class.recommend_model(year,year+1)
    
    def recommend(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            portfolio_class.recommend()

    def simulate(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            strat_class = portfolio_class.strat_class
            price_db = strat_class.price_db
            self.market.connect()
            prices = self.market.retrieve(price_db)
            self.market.disconnect()
            prices = p.column_date_processing(prices)
            portfolio_class.db_subscribe()
            simmer = Simulator(portfolio_class)
            simmer.simulate(prices.copy())
            portfolio_class.db_unsubscribe()
        
    def visualize(self):
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            strat_class = portfolio_class.strat_class
            price_db = strat_class.price_db
            self.market.connect()
            prices = self.market.retrieve(price_db)
            self.market.disconnect()
            prices = p.column_date_processing(prices)
            portfolio_class.db_subscribe()
            viz = Visualizer(portfolio_class)
            viz.visualize(prices.copy())
            portfolio_class.db_unsubscribe()
    
    def reset(self):
        for portfolio in self.portfolios:
            try:
                portfolio_class = portfolio["portfolio_class"]
                portfolio_class.db.connect()
                portfolio_class.db.drop(f"trades")
                portfolio_class.db.drop(f"portfolios")
                portfolio_class.db.drop(f"optimal_parameters")
                portfolio_class.db.disconnect()
            except Exception as e:
                print(str(e))
                continue
    
    def pull_trades(self):
        trades = []
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            portfolio_class.db_subscribe()
            portfolio_trades = portfolio_class.pull_trades()
            portfolio_trades["strategy"] = portfolio_class.strat_class.name
            trades.append(portfolio_trades)
            portfolio_class.db_unsubscribe()
        return trades
    
    def pull_portfolio_states(self):
        portfolios = []
        for portfolio in self.portfolios:
            portfolio_class = portfolio["portfolio_class"]
            portfolio_class.db_subscribe()
            portfolio_trades = portfolio_class.pull_portfolios()
            portfolio_trades["strategy"] = portfolio_class.strat_class.name
            portfolios.append(portfolio_trades)
            portfolio_class.db_unsubscribe()
        return portfolios