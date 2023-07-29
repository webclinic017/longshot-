from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.aipricer import AIPricer


class Speculation(AIPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "speculation"
        
    def training_set(self,ticker,prices,current):
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year",self.time_horizon_class.naming_convention]).mean().reset_index()
        for i in range(14):
            ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        if not current:
            ticker_data["y"] = ticker_data["adjclose"].shift(-self.time_horizon_class.y_pivot_number)
            columns = self.all_columns
        else:
            columns = self.all_columns[:-1]
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        ticker_data = ticker_data[columns]
        return ticker_data