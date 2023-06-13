from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.aipricer import AIPricer


class Speculation(AIPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_speculation"
        self.db = ADatabase(self.name)
        self.factors = [str(x) for x in range(14)]
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 20 if asset_class.value == "stocks" else 1
        
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
    
    def price_returns(self,ticker):
        prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
        ticker_sim = p.column_date_processing(prices)
        for i in range(2,5):
            ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"].shift(-1)) / ticker_sim["adjclose"].shift(-1)
            ticker_sim[f"risk_return_{i}"] = (ticker_sim["adjclose"].shift(5) - ticker_sim["adjclose"].shift(i+5)) / ticker_sim["adjclose"].shift(i+5)
        ticker_sim["weekly_return"] = ticker_sim["return_4"]
        ticker_sim["weekly_risk_return"] = ticker_sim["risk_return_4"]
        return ticker_sim

    def risk_returns(self,ticker):
        prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
        ticker_sim = p.column_date_processing(prices)
        for i in range(2,5):
            ticker_sim[f"risk_return_{i}"] = (ticker_sim["adjclose"] - ticker_sim["adjclose"].shift(i)) / ticker_sim["adjclose"].shift(i)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        ticker_sim = ticker_sim[ticker_sim["day"]==4]
        ticker_sim["weekly_risk_return"] = ticker_sim["risk_return_4"]
        ticker_sim["week"] = ticker_sim["week"] + 1
        return ticker_sim
    
    @classmethod
    def transform(self,ticker_data):
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year","week"]).mean().reset_index()
        for i in range(14):
            ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
        return ticker_data