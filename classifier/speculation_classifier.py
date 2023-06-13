from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from classifier.aiclassifier import AIClassifier


class SpeculationClassifier(AIClassifier):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_speculation_classification"
        self.db = ADatabase(self.name)
        self.factors = ["d1","d2","d3","rolling14"]
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 20 if asset_class.value == "stocks" else 1
        
    def training_set(self,ticker,prices,current):
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year",self.time_horizon_class.naming_convention]).mean().reset_index()
        ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
        ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
        ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
        ticker_data["rolling14"] = ticker_data["adjclose"].rolling(window=14).mean()
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        if not current:
            ticker_data["future"] = ticker_data["adjclose"].shift(-self.time_horizon_class.y_pivot_number)
            ticker_data["delta"] = (ticker_data["future"] - ticker_data["adjclose"]) / ticker_data["adjclose"]
            ticker_data["y"] = [x > 0 for x in ticker_data["delta"]]
            columns = self.all_columns
        else:
            columns = self.all_columns[:-1]
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        ticker_data = ticker_data[columns]
        return ticker_data