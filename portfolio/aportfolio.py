from pricer.pricer_factory import PricerFactory as pricer_fact
from classifier.classifier_factory import ClassifierFactory as classifier_fact
from ranker.ranker_factory import RankerFactory as ranker_fact
from modeler_strats.universal_modeler import UniversalModeler
from risk.beta_risk import BetaRisk
from returns.required_returns import RequiredReturn
from database.adatabase import ADatabase
from backtester.abacktester import ABacktester
import pandas as pd
from returns.products import Products

class APortfolio(object):

    def __init__(self,pricer,classifier,ranker):
        self.pricer = pricer
        self.classifier = classifier
        self.ranker = ranker
        self.risk = BetaRisk()
        self.returns = RequiredReturn()
        self.modeler_strat = UniversalModeler()
        self.classifier_name = self.classifier.name if self.classifier.name != None else str(None)
        self.ranker_name = self.ranker.name if self.ranker.name != None else str(None)
        self.names = [self.pricer.name,self.classifier_name,self.ranker_name,self.risk.name,self.returns.name]
        self.acronyms = ["".join([subname[0] for subname in x.split("_")]) for x in self.names]
        self.name = "_".join(self.acronyms).lower()
        self.db = ADatabase(self.name)
    
    def load_optimal_parameter(self,parameter):
        self.parameter = parameter

    def initialize(self,backtest_start_date,backtest_end_date,current_start_date):
        self.backtest_start_date = backtest_start_date
        self.backtest_end_date = backtest_end_date
        self.current_start_date = current_start_date
        self.pricer_class = pricer_fact.build(self.pricer)
        self.ranker_class = ranker_fact.build(self.ranker)
        self.classifier_class = classifier_fact.build(self.classifier)
    
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
        self.pricer_class.db.connect()
        sim = self.pricer_class.db.retrieve("sim")
        self.pricer_class.db.disconnect()
        return sim
    
    def pull_classifier_sim(self):
        if self.classifier_class == None:
            return pd.DataFrame([{}])
        else:
            self.classifier_class.db.connect()
            sim = self.classifier_class.db.retrieve("sim")
            self.classifier_class.db.disconnect()
            return sim
        
    def pull_ranker_sim(self):
        if self.ranker_class == None:
            return pd.DataFrame([{}])
        else:
            self.ranker_class.db.connect()
            sim = self.ranker_class.db.retrieve("sim")
            self.ranker_class.db.disconnect()
            return sim
    
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

    def create_returns(self,market,bench,current):
        new_prices = []
        sp500 = self.pricer_class.sp500.copy()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        tickers = ["BTC"] if self.pricer_class.asset_class.value == "crypto" else sp500["ticker"].unique()
        for ticker in tickers[:1]:
            try:
                ticker_sim = market.retrieve_ticker_prices(self.pricer_class.asset_class.value,ticker)
                ticker_sim = self.pricer_class.price_returns(ticker_sim,current)
                print(ticker_sim.head())
                completed = self.risk.risk(self.pricer_class.time_horizon_class,ticker_sim,bench)
                print(completed.head())
                new_prices.append(completed)
            except Exception as e:
                print(str(e))
                continue
        price_returns = pd.concat(new_prices)
        return price_returns
    
    def merge_sim_returns(self,sim,returns):
        merged = sim.merge(returns,on=["year",self.pricer_class.time_horizon_class.naming_convention,"ticker"],how="left")
        return merged
    
    def run_backtest(self,market,simulation,parameter,rec):
        tyield_name = parameter["tyields"]
        market.connect()
        tyields = market.retrieve(tyield_name)
        market.disconnect()
        tyields = Products.tyields(tyields)
        return self.backtester.backtest(simulation,tyields,parameter,rec)
             
    def pull_orders(self):
        self.db.cloud_connect()
        orders = self.db.retrieve("orders")
        self.db.disconnect()
        return orders
    
    def pull_historical_trades(self):
        self.db.connect()
        trade = self.db.retrieve("historical_trades")
        self.db.disconnect()
        trade["strat"] = self.name
        trade["positions"] = self.pricer_class.positions
        return trade
    
    def initialize_backtester(self,start_date,end_date):
        self.backtester = ABacktester(self,True,start_date,end_date)

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