from time_horizons.time_horizons_factory import TimeHorizonFactory
from database.market import Market
from database.adatabase import ADatabase
from processor.processor import Processor as p
import numpy as np
import pandas as pd
# description: class for data products
class ADataProduct(object):
    
    def __init__(self,asset_class,time_horizon):
        self.time_horizon_class = TimeHorizonFactory.build(time_horizon)
        self.asset_class = asset_class
        self.market = Market()
        # self.pull_sp100()
        self.pull_sp100()
    
    def initialize(self):
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_{self.naming_suffix}"
        self.db = ADatabase(self.name)
    
    # def pull_sp100(self):
    #     self.market.connect()
    #     self.sp100 = self.market.retrieve("sp100")
    #     self.market.disconnect()
    
    def pull_sp100(self):
        self.market.connect()
        self.sp100 = self.market.retrieve("sp100")
        self.market.disconnect()
        
    def pull_sim(self):
        self.db.connect()
        sim = self.db.retrieve("sim")
        self.db.disconnect()
        return sim
    
    def pull_predictions(self):
        self.db.connect()
        predictions = self.db.retrieve("predictions")
        self.db.disconnect()
        return predictions
    
    def pull_recs(self):
        self.db.connect()
        predictions = self.db.retrieve("recs")
        self.db.disconnect()
        return predictions
    
    def drop_sim(self):
        self.db.connect()
        self.db.drop("sim")
        self.db.disconnect()
        
    def drop_predictions(self):
        self.db.connect()
        self.db.drop("predictions")
        self.db.disconnect()
    
    def drop_recommendations(self):
        self.db.connect()
        self.db.drop("recs")
        self.db.disconnect()
    
    def training_set(self):
        self.market.connect()
        training_sets = []
        for ticker in self.sp100["ticker"].unique():
            try:
                prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
                ticker_data = p.column_date_processing(prices)
                ticker_data.sort_values("date",inplace=True)
                ticker_data["adjopen"] = [float(x) for x in ticker_data["adjopen"]]
                ticker_data = self.training_set_helper(ticker_data,False)
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                training_sets.append(ticker_data)
            except Exception as e:
                print(str(e))
                continue
        self.market.disconnect()
        data = pd.concat(training_sets)
        training_data = data.dropna().copy().sort_values("date")
        self.training_data = training_data