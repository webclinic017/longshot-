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
class ATradeAlgorithm(object):

    def __init__(self,returns,risk):
        self.returns = returns
        self.risk = risk

    def load_optimal_parameter(self):
        self.db.connect()
        self.parameter = self.db.retrieve("optimal").to_dict("records")[0]
        self.db.disconnect()

    def pull_iterations(self):
        self.db.connect()
        iterations = self.db.retrieve("iterations")
        self.db.disconnect()
        return iterations
    
    def initialize(self,pricer,ranker,classifier,backtest_start_date,backtest_end_date,current_start_date):
        self.backtest_start_date = backtest_start_date
        self.backtest_end_date = backtest_end_date
        self.current_start_date = current_start_date
        self.pricer_class = pricer_fact.build(pricer)
        self.ranker_class = ranker_fact.build(ranker)
        self.classifier_class = classifier_fact.build(classifier)
        self.analysis = Analysis(self.pricer_class.time_horizon_class.name)
        self.market_return = 1.15
        self.positions = 20 if self.pricer_class.asset_class.value == "stocks" else 1

    def initialize_classes(self):
        self.pricer_class.initialize()
        self.classifier_name = self.classifier_class.name if self.classifier_class != None else str(None)
        self.ranker_name = self.ranker_class.name if self.ranker_class != None else str(None)
        self.names = [self.pricer_class.name,self.classifier_name,self.ranker_name,self.risk.name,self.returns.name]
        self.acronyms = ["".join([subname[0] for subname in x.split("_")]) for x in self.names]
        self.name = "_".join(self.acronyms).lower()
        self.db = ADatabase(self.name)

    def initialize_bench_and_yields(self):
        self.pricer_class.market.connect()
        self.benchmark = Products.spy_bench(self.pricer_class.market.retrieve("spy"))
        self.tyields = Products.tyields(self.pricer_class.market.retrieve("tyields"),1)
        self.tyields2 = Products.tyields(self.pricer_class.market.retrieve("tyields2"),2)
        self.tyields10 = Products.tyields(self.pricer_class.market.retrieve("tyields10"),10)
        self.pricer_class.market.disconnect()
        dropped_cols = ["realtime_start","realtime_end","value"]
        self.yields = self.tyields.merge(self.tyields2.drop(dropped_cols,axis=1),on=["year","quarter","month","week","date"],how="left") \
                    .merge(self.tyields10.drop(dropped_cols,axis=1),on=["year","quarter","month","week","date"],how="left")

    def create_simulation(self):
        sims = []
        pricer_sim = self.pull_pricer_sim()[["year",self.pricer_class.time_horizon_class.naming_convention,"ticker","price_prediction"]]
        if self.pricer_class.time_horizon_class.naming_convention == "week":
            pricer_sim = pricer_sim[pricer_sim["week"]< 54]
        if self.ranker_class != None:
            ranker_sim = self.pull_ranker_sim()[["year",self.pricer_class.time_horizon_class.naming_convention,"ticker","rank_prediction"]]
            sims.append(ranker_sim)
        if self.classifier_class != None:
            classifier_sim = self.pull_classifier_sim()[["year",self.pricer_class.time_horizon_class.naming_convention,"ticker","classification_prediction"]]
            classifier_sim.dropna(inplace=True)
            classifier_sim["classification_prediction"] = [self.ameme(x) for x in classifier_sim["classification_prediction"]]
            sims.append(classifier_sim)
        for sim in sims:
            if sim.index.size > 0:
                pricer_sim = pricer_sim.merge(sim,on=["year",self.pricer_class.time_horizon_class.naming_convention,"ticker"],how="left")
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
        
    def create_current_simulation(self):
        sims = []
        pricer_sim = self.pull_pricer_predictions()[["year",self.pricer_class.time_horizon_class.naming_convention,"ticker","price_prediction"]]
        if self.pricer_class.time_horizon_class.naming_convention == "week":
            pricer_sim = pricer_sim[pricer_sim["week"]< 54]
        if self.ranker_class != None:
            ranker_sim = self.pull_ranker_predictions()[["year",self.pricer_class.time_horizon_class.naming_convention,"ticker","rank_prediction"]]
            sims.append(ranker_sim)
        if self.classifier_class != None:
            classifier_sim = self.pull_classifier_predictions()[["year",self.pricer_class.time_horizon_class.naming_convention,"ticker","classification_prediction"]]
            classifier_sim.dropna(inplace=True)
            classifier_sim["classification_prediction"] = [self.ameme(x) for x in classifier_sim["classification_prediction"]]
            sims.append(classifier_sim)
        for sim in sims:
            if sim.index.size > 0:
                pricer_sim = pricer_sim.merge(sim,on=["year",self.pricer_class.time_horizon_class.naming_convention,"ticker"],how="left")
        return pricer_sim
    
    def pull_pricer_predictions(self):
        self.pricer_class.db.connect()
        predictions = self.pricer_class.db.retrieve("predictions")
        self.pricer_class.db.disconnect()
        return predictions
    
    def pull_classifier_predictions(self):
        if self.classifier_class == None:
            return pd.DataFrame([{}])
        else:
            self.classifier_class.db.connect()
            predictions = self.classifier_class.db.retrieve("predictions")
            self.classifier_class.db.disconnect()
            return predictions
        
    def pull_ranker_predictions(self):
        if self.ranker_class == None:
            return pd.DataFrame([{}])
        else:
            self.ranker_class.db.connect()
            predictions = self.ranker_class.db.retrieve("predictions")
            self.ranker_class.db.disconnect()
            return predictions

    def create_returns(self,current):
        new_prices = []
        sp500 = self.pricer_class.sp500.copy()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        tickers = ["BTC"] if self.pricer_class.asset_class.value == "crypto" else sp500["ticker"].unique()
        self.pricer_class.market.connect()
        for ticker in tickers:
            try:
                ticker_sim = self.pricer_class.market.retrieve_ticker_prices(self.pricer_class.asset_class.value,ticker)
                ticker_sim = self.pricer_class.price_returns(ticker_sim,current)
                completed = self.risk.risk(self.pricer_class.time_horizon_class,ticker_sim,self.benchmark)
                new_prices.append(completed)
            except Exception as e:
                print(str(e))
                continue
        self.pricer_class.market.disconnect()
        price_returns = pd.concat(new_prices)
        return price_returns
    
    def merge_sim_returns(self,sim,returns):
        merged = sim.merge(returns,on=["year",self.pricer_class.time_horizon_class.naming_convention,"ticker"],how="left")
        return merged
    
    def apply_yields(self,sim,rec):
        final_data = self.returns.returns(self.market_return,self.pricer_class.time_horizon_class,sim,rec,self.yields)
        return final_data
    
    def initialize_backtester(self):
        self.parameters = params.parameters()
        self.backtester = ABacktester(self,True,self.backtest_start_date,self.backtest_end_date)
    
    def run_backtest(self,simulation):
        trades = []
        self.db.connect()
        self.db.create_index("trades","iteration")
        for i in tqdm(range(len(self.parameters))):
            try:
                parameter = self.parameters[i]
                parameter["iteration"] = i
                trade = self.backtester.backtest(simulation.copy(),parameter,False)
                self.db.store("iterations",pd.DataFrame([parameter]))
                self.db.store("trades",trade)
                trades.append(trade)
            except Exception as e:
                print(str(e))
        # self.db.create_index("trades","iteration")
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
             
    def pull_orders(self):
        self.db.cloud_connect()
        orders = self.db.retrieve("orders")
        self.db.disconnect()
        return orders
    
    def pull_recommendations(self):
        self.db.connect()
        recs = self.db.retrieve("recs")
        self.db.disconnect()
        return recs
    
    def pull_historical_trades(self):
        self.db.connect()
        trade = self.db.retrieve("historical_trades")
        self.db.disconnect()
        trade["strat"] = self.name
        trade["positions"] = self.pricer_class.positions
        return trade

    def pull_trades(self):
        self.db.connect()
        trade = self.db.retrieve("trades")
        self.db.disconnect()
        trade["strat"] = self.name
        trade["positions"] = self.pricer_class.positions
        return trade
    
    def reset(self):
        self.db.connect()
        self.db.drop_all()
        self.db.disconnect()