from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.nonaipricer import NonAIPricer

class Window(NonAIPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_window"
        self.db = ADatabase(self.name)
        self.factors = []
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 10 if asset_class.value == "stocks" else 1
    
    def training_set(self):
        self.db.connect()
        training_sets = self.db.retrieve("sim")
        self.db.disconnect()
        tickers = ["BTC"] if self.asset_class.value == "crypto" else list(self.sp500["ticker"].unique()[:10])
        if training_sets.index.size < 1:
            training_set_dfs = []
            self.market.connect()
            for ticker in tickers:
                try:
                    prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
                    prices = p.column_date_processing(prices)
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
                    training_set_dfs.append(ticker_data)
                except Exception as e:
                    print(str(e))
                    continue  
            self.market.disconnect() 
            training_sets = pd.concat(training_set_dfs)
            self.db.connect()
            self.db.store("sim",training_sets)
            self.db.disconnect()
    
    def price_returns(self,ticker):
        prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
        ticker_sim = p.column_date_processing(prices)
        for i in range(2,5):
            ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"].shift(-1)) / ticker_sim["adjclose"].shift(-1)
        # ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        ticker_sim["weekly_return"] = ticker_sim["return_4"]
        return ticker_sim  