from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from strategy.aistrategy import AIStrategy

class Speculation(AIStrategy):

    def __init__(self,asset_class,quarterly):
        super().__init__(asset_class,quarterly)
        self.name = f"{self.group_timeframe}ly_{asset_class}_speculation"
        self.db = ADatabase(self.name) if quarterly else ADatabase(self.name)
        self.factors = [str(x) for x in range(14)]
        self.included_columns = ["year","week","ticker","adjclose","y"]
        self.included_live_columns = ["year","week","ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 10 if asset_class == "prices" else 1
        
    def training_set(self,market,sec,sp500):
        training_sets = []
        tickers = ["BTC"] if self.asset_class.value == "crypto" else list(sp500["ticker"].unique()[:10])
        for ticker in tickers:
            try:
                prices = market.retrieve_ticker_prices(self.asset_class,ticker)
                prices = p.column_date_processing(prices)
                ticker_data = prices[prices["ticker"]==ticker]
                ticker_data.sort_values("date",ascending=True,inplace=True)
                ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                ticker_data = ticker_data.groupby(["year",self.group_timeframe]).mean().reset_index()
                for i in range(14):
                    ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
                ticker_data.dropna(inplace=True)
                ticker_data["ticker"] = ticker
                ticker_data["y"] = ticker_data["adjclose"].shift(-self.projection_horizon)
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                ticker_data = ticker_data[self.all_columns]
                training_sets.append(ticker_data)
            except:
                continue  
        return pd.concat(training_sets)
    
    @classmethod
    def factors(self):
        return [str(x) for x in range(14)]
    
    @classmethod
    def transform(self,ticker_data):
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year","week"]).mean().reset_index()
        for i in range(14):
            ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
        return ticker_data