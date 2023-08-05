from database.adatabase import ADatabase
from database.market import Market
from database.sec import SEC
from portfolio.portfolio_fact import PortfolioFactory
from strats.strat_fact import StratFactory as stratfact
from diversifier.diversifier_fact import DiversifierFactory as diversifyfact
from simulator.simulator import Simulator
from processor.processor import Processor as p
from visualizer.visualizer import Visualizer
from datetime import datetime
from backtester.backtester import Backtester
from tqdm import tqdm
class Fund(object):

    def __init__(self,name,state):
        self.name = name
        self.db = ADatabase(name)
        self.state = state
        self.market = Market()
        self.sec = SEC()
        self.start_year = 2016
        self.end_year = 2023
        self.start_date = datetime(2016,1,1)
        self.end_date = datetime(2023,1,1)
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
            portfolio_type = portfolio["portfolio_type"]
            modeler_type = portfolio["modeler_type"]
            diversifier_type = portfolio["diversifier_type"]
            strat_class = stratfact.build_strat(portfolio_name)
            modeler_class = modelstratfact.build_modeler_strat(modeler_type)
            diversifier_class = diversifyfact.build_diversifier(diversifier_type)
            port_class = PortfolioFactory.build_portfolio(portfolio_type,strat_class,modeler_class,diversifier_class,positions,self.state)
            portfolio["portfolio_class"] = port_class
    
    def transform(self):
        for portfolio in self.portfolios:
            strat_class = portfolio["portfolio_class"].strat_class
            strat_class.transform(self.market,self.sec)
    
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
            sp500 = self.market.retrieve("sp500")
            self.market.disconnect()
            prices = p.column_date_processing(prices)
            prices["year"] = [x.year for x in prices["date"]]
            prices["quarter"] = [x.quarter for x in prices["date"]]
            prices = p.column_date_processing(prices)
            prices = prices.merge(sp500[["Symbol","GICS Sector","GICS Sub-Industry"]].rename(columns={"Symbol":"ticker"}),on="ticker",how="left")
            strat_class.db.connect()
            modeler_class = portfolio_class.modeler_class
            modeler_type = modeler_class.name
            simulation = strat_class.db.retrieve(f"{modeler_type}_sim")
            strat_class.db.disconnect()
            sim = strat_class.create_sim(prices.copy(),simulation)
            backtester = Backtester(self.start_date,self.end_date,portfolio_class,self.initial)
            backtester.parameters_init()
            backtester.backtest(sim.copy(),prices)
    
    def recommend_transform(self):
        for portfolio in self.portfolios:
            strat_class = portfolio["portfolio_class"].strat_class
            strat_class.recommend_transform(self.market,self.sec)
    
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
        for portfolio in tqdm(self.portfolios):
            portfolio_class = portfolio["portfolio_class"]
            strat_class = portfolio_class.strat_class
            price_db = strat_class.price_db
            self.market.connect()
            prices = self.market.retrieve(price_db)
            sp500 = self.market.retrieve("sp500")
            self.market.disconnect()
            prices = p.column_date_processing(prices)
            prices = prices.merge(sp500[["Symbol","GICS Sector","GICS Sub-Industry"]].rename(columns={"Symbol":"ticker"}),on="ticker",how="left")
            portfolio_class.db_subscribe()
            simmer = Simulator(portfolio_class)
            simmer.simulate(prices.copy())
            portfolio_class.db_unsubscribe()
        
    def visualize(self):
        for portfolio in tqdm(self.portfolios):
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
                portfolio_class.db.drop(f"visualization")
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
            portfolio_trades["portfolio_type"] = portfolio["portfolio_type"]
            portfolio_trades["modeler_type"] = portfolio["modeler_type"]
            portfolio_trades["diversifier_type"] = portfolio["diversifier_type"]
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
            portfolio_trades["portfolio_type"] = portfolio["portfolio_type"]
            portfolio_trades["modeler_type"] = portfolio["modeler_type"]
            portfolio_trades["diversifier_type"] = portfolio["diversifier_type"]
            portfolios.append(portfolio_trades)
            portfolio_class.db_unsubscribe()
        return portfolios