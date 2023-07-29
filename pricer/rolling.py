from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.nonaipricer import NonAIPricer

class Rolling(ATradingPricer):
    
    # test

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "rolling"

    def training_set(self,ticker,prices,current):
        ticker_data = prices[prices["ticker"]==ticker]
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data[f"price_prediction"] = ticker_data["adjclose"].rolling(100).mean()
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        if not current:
            ticker_data["y"] = ticker_data["adjclose"].shift(-self.time_horizon_class.y_pivot_number)
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        ticker_data = ticker_data[["year","quarter",self.time_horizon_class.naming_convention,"ticker",f"price_prediction"]]
        return ticker_data