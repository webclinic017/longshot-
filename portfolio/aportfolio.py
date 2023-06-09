from pricer.pricer_factory import PricerFactory as pricer_fact
from classifier.classifier_factory import ClassifierFactory as classifier_fact
from ranker.ranker_factory import RankerFactory as ranker_fact
from modeler_strats.universal_modeler import UniversalModeler
from risk.beta_risk import BetaRisk
from returns.required_returns import RequiredReturn
from database.adatabase import ADatabase
from backtester.abacktester import ABacktester
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

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
            classifier_sim["classification_prediction"] = [int(x) for x in classifier_sim["classification_prediction"]]
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
    
    def create_returns(self):
        new_prices = []
        sp500 = self.pricer_class.sp500.copy()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        tickers = ["BTC"] if self.pricer_class.asset_class == "crypto" else sp500["ticker"].unique()
        for ticker in tqdm(tickers):
            try:
                ticker_sim = self.pricer_class.price_returns(ticker)
                completed = self.risk.risk(self.pricer_class.time_horizon_class,ticker_sim)
                new_prices.append(completed)
            except Exception as e:
                print(str(e))
                continue
        price_returns = pd.concat(new_prices)
        return price_returns
    
    def merge_sim_returns(self,sim,returns):
        merged = sim.merge(returns,on=["year",self.pricer_class.time_horizon_class.naming_convention,"ticker"],how="left")
        merged = self.returns.returns(self.pricer_class.time_horizon_class,merged)
        time_frame = self.pricer_class.time_horizon_class.naming_convention
        if time_frame == "quarter":
            merged["date"] = [datetime(row[1]["year"],row[1]["quarter"]*3 - 2, 1) for row in merged.iterrows()]
        else:
            merged["date_string"] = [f'{int(row[1]["year"])}-W{int(row[1]["week"])}' for row in merged.iterrows()]
            merged["date"] = [datetime.strptime(x + '-1', '%G-W%V-%u') for x in merged["date_string"]]
        return merged
    
    def backtest_rank(self,sim):
        return self.ranker_class.backtest_rank(sim)
    
    def initialize_historical_backtester(self,start_date,end_date):
        self.backtester = ABacktester(self,False,start_date,end_date)

    def run_historical_backtest(self,simulation):
        # print(simulation.columns)
        self.backtester.backtest(simulation)
        
    def pull_historical_trades(self):
        self.db.connect()
        trade = self.db.retrieve("historical_trades")
        self.db.disconnect()
        trade["strat"] = self.name
        trade["positions"] = self.pricer_class.positions
        return trade
    
    def reset(self):
        self.db.connect()
        self.db.drop_all()
        self.db.disconnect()