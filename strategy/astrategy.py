from pricer.pricer_factory import PricerFactory as pricer_fact
from classifier.classifier_factory import ClassifierFactory as classifier_fact
from analysis.analysis import Analysis
from ranker.ranker_factory import RankerFactory as ranker_fact
from backtester.abacktester import ABacktester
import pandas as pd
from returns.products import Products
from parameters.parameters import Parameters as params
from database.adatabase import ADatabase
from tqdm import tqdm

class AStrategy(object):

    def __init__(self,returns,risk):
        self.returns = returns
        self.risk = risk
    
    def initialize(self,pricer,ranker,classifier,backtest_start_date,backtest_end_date,current_start_date):
        self.backtest_start_date = backtest_start_date
        self.backtest_end_date = backtest_end_date
        self.current_start_date = current_start_date
        self.pricer_class = pricer_fact.build(pricer)
        self.ranker_class = ranker_fact.build(ranker)
        self.classifier_class = classifier_fact.build(classifier)
        self.analysis = Analysis(self.pricer_class.time_horizon_class.name)
        self.market_return = 1.15
        self.positions = 10

    def initialize_classes(self):
        self.pricer_class.initialize()
        self.ranker_class.initialize()
        self.classifier_class.initialize()
        self.names = [self.pricer_class.name,self.classifier_class.name,self.ranker_class.name,self.risk.name,self.returns.name]
        self.acronyms = ["".join([subname[0] for subname in x.split("_")]) for x in self.names]
        self.name = "strategy_" + "_".join(self.acronyms).lower()
        self.db = ADatabase(self.name)

    def initialize_bench_and_yields(self):
        self.pricer_class.market.connect()
        self.benchmark = Products.spy_bench(self.pricer_class.market.retrieve("spy"),self.pricer_class.time_horizon_class)
        self.tyields = Products.tyields(self.pricer_class.market.retrieve("tyields"),1,self.pricer_class.time_horizon_class)
        self.tyields2 = Products.tyields(self.pricer_class.market.retrieve("tyields2"),2,self.pricer_class.time_horizon_class)
        self.tyields10 = Products.tyields(self.pricer_class.market.retrieve("tyields10"),10,self.pricer_class.time_horizon_class)
        self.pricer_class.market.disconnect()
        dropped_cols = ["realtime_start","realtime_end","value"]
        self.yields = self.tyields.merge(self.tyields2.drop(dropped_cols,axis=1,errors="ignore"),on=["year","quarter","month","week","date"],how="left") \
                    .merge(self.tyields10.drop(dropped_cols,axis=1,errors="ignore"),on=["year","quarter","month","week","date"],how="left")

    def create_simulation(self):
        sims = []
        pricer_sim = self.pull_pricer_sim()[["date","ticker","price"]]
        if self.ranker_class != None:
            ranker_sim = self.pull_ranker_sim()[["date","ticker","rank"]]
            sims.append(ranker_sim)
        if self.classifier_class != None:
            classifier_sim = self.pull_classifier_sim()[["date","ticker","classification"]]
            classifier_sim["classification"] = [self.ameme(x) for x in classifier_sim["classification"]]
            sims.append(classifier_sim)
        for sim in sims:
            if sim.index.size > 0:
                pricer_sim = pricer_sim.merge(sim,on=["date","ticker"],how="left")
        return pricer_sim
    
    def pull_pricer_sim(self):
        return self.pricer_class.pull_sim()
    
    def pull_classifier_sim(self):
        if self.classifier_class == None:
            return pd.DataFrame([{}])
        else:
            return self.classifier_class.pull_sim()
        
    def pull_ranker_sim(self):
        if self.ranker_class == None:
            return pd.DataFrame([{}])
        else:
            return self.ranker_class.pull_sim()
    
    def ameme(self,x):
        try:
            return int(x)
        except:
            return 0
    
    def create_returns(self,current):
        new_prices = []
        sp100 = self.pricer_class.sp100.copy()
        tickers = ["BTC"] if self.pricer_class.asset_class.value == "crypto" else sp100["ticker"].unique()
        self.pricer_class.market.connect()
        for ticker in tickers:
            try:
                ticker_sim = self.pricer_class.market.retrieve_ticker_prices(self.pricer_class.asset_class.value,ticker)
                ticker_sim = self.returns.returns(self.pricer_class.time_horizon_class,ticker_sim,current)
                new_prices.append(ticker_sim)
                # completed = self.risk.risk(self.pricer_class.time_horizon_class,ticker_sim,self.benchmark)
                # new_prices.append(completed)
            except Exception as e:
                print(str(e))
                continue
        self.pricer_class.market.disconnect()
        price_returns = pd.concat(new_prices)
        return price_returns
    
    def merge_sim_returns(self,sim,returns):
        merged = sim.merge(returns,on=["date","ticker"],how="left")
        merged = merged.merge(self.pricer_class.sp100[["ticker","GICS Sector"]],on="ticker",how="left")
        return merged
    
    def apply_yields(self,sim,rec):
        final_data = self.returns.required_returns(self.market_return,self.pricer_class.time_horizon_class,sim,rec,self.yields)
        return final_data
    
    def initialize_backtester(self):
        self.parameters = params.parameters()
        self.backtester = ABacktester(self,True,self.backtest_start_date,self.backtest_end_date)
    
    def run_backtest(self,simulation):
        trades = []
        self.db.connect()
        self.db.create_index("trades","iteration")
        for i in range(len(self.parameters)):
            try:
                parameter = self.parameters[i]
                parameter["iteration"] = i
                trade = self.backtester.backtest(simulation.copy(),parameter,False)
                self.db.store("iterations",pd.DataFrame([parameter]))
                self.db.store("trades",trade)
                trades.append(trade)
            except Exception as e:
                print(str(e))
        self.db.disconnect()
        return pd.concat(trades)

    def run_recommendation(self,simulation):
        trades = []
        self.db.connect()
        try:
            parameter = self.parameter
            trade = self.backtester.backtest(simulation.copy(),parameter,True)
            self.db.store("recs",trade)
            trades.append(trade)
        except Exception as e:
            print(str(e))
        self.db.disconnect()
        return pd.concat(trades)
    
    def run_performance(self,simulation):
        trades = []
        self.db.connect()
        self.db.create_index("trades","iteration")
        parameter = self.parameter
        try:
            trade = self.backtester.backtest(simulation.copy(),parameter,False)
            self.db.store("performance",trade)
            trades.append(trade)
        except Exception as e:
            print(str(e))
        self.db.disconnect()
        return pd.concat(trades)
    
    def load_optimal_parameter(self):
        self.db.connect()
        self.parameter = self.db.retrieve("optimal").to_dict("records")[0]
        self.db.disconnect()

    def pull_iterations(self):
        self.db.connect()
        iterations = self.db.retrieve("iterations")
        self.db.disconnect()
        return iterations      
     
    def pull_recommendations(self):
        self.db.connect()
        recs = self.db.retrieve("recs")
        self.db.disconnect()
        return recs

    def pull_trades(self):
        self.db.connect()
        trade = self.db.retrieve("trades")
        self.db.disconnect()
        trade["strat"] = self.name
        trade["positions"] = self.positions
        return trade
    
    def pull_performance(self):
        self.db.connect()
        trade = self.db.retrieve("performance")
        self.db.disconnect()
        trade["strat"] = self.name
        trade["positions"] = self.positions
        return trade
    
    def drop_performance(self):
        self.db.connect()
        recs = self.db.drop("performance")
        self.db.disconnect()
        return recs
    
    def drop_recommendations(self):
        self.db.connect()
        recs = self.db.drop("recs")
        self.db.disconnect()
        return recs   

    def drop_trades(self):
        self.db.connect()
        recs = self.db.drop("trades")
        self.db.disconnect()
        return recs   
    
    def drop_iterations(self):
        self.db.connect()
        recs = self.db.drop("iterations")
        self.db.disconnect()
        return recs   