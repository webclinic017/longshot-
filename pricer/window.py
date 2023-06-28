from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.aweeklypricer import AWeeklyPricer

class Window(AWeeklyPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_window"
        self.db = ADatabase(self.name)
        self.factors = []
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.isai = False
    
    def training_set(self,ticker,prices):
        ticker_data = prices[prices["ticker"]==ticker]
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data[f"price_prediction"] = ticker_data["adjclose"].shift(10)
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        ticker_data["y"] = ticker_data["adjclose"].shift(-self.time_horizon_class.y_pivot_number)
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        ticker_data = ticker_data[["year","quarter","week","ticker",f"price_prediction"]]
        return ticker_data