from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from ranker.nonairanker import NonAIRanker

class FastSlow(NonAIRanker):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_fastslow_ranker"
        self.db = ADatabase(self.name)
        self.factors = []
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjopen","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjopen","y"]
        self.all_columns = self.factors + self.included_columns
        
    def training_set(self,ticker,prices):
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjopen"] = [float(x) for x in ticker_data["adjopen"]]
        ticker_data[f"fast"] = ticker_data["adjopen"].rolling(50).mean()
        ticker_data[f"slow"] = ticker_data["adjopen"].rolling(200).mean()
        ticker_data["rank_prediction"] = (ticker_data["fast"] - ticker_data["slow"]) / ticker_data["slow"]
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        ticker_data["y"] = ticker_data["adjopen"].shift(-self.time_horizon_class.y_pivot_number)
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        ticker_data = ticker_data[["year","quarter","week","ticker",f"rank_prediction"]]
        return ticker_data
    
    def backtest_rank(self,simulation):
        filtered = simulation[(simulation["rank_prediction"] <= simulation["rank_prediction"].mean()) & (simulation["rank_prediction"] > 0)]
        return filtered