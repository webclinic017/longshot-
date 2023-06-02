from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from strategy.nonaistrategy import NonAIStrategy

class Rolling(NonAIStrategy):

    def __init__(self,asset_class,quarterly):
        super().__init__(asset_class,quarterly)
        self.name = f"{self.group_timeframe}ly_{asset_class}_rolling"
        self.db = ADatabase(self.name)
        self.factors = [str(x) for x in range(14)]
        self.included_columns = ["year","week","ticker","adjclose","y"]
        self.included_live_columns = ["year","week","ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 10 if asset_class == "prices" else 1
        
    def create_sim(self,market,sec,sp500):
        sim = []
        tickers = ["BTC"] if self.asset_class == "crypto" else list(sp500["ticker"].unique()[:10])
        for ticker in tickers:
            try:
                prices = market.retrieve_ticker_prices(self.asset_class,ticker)
                prices = p.column_date_processing(prices)
                ticker_data = prices[prices["ticker"]==ticker]
                ticker_data.sort_values("date",ascending=True,inplace=True)
                ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                ticker_data[f"{self.name}_prediction"] = ticker_data["adjclose"].rolling(100).mean()
                ticker_data.dropna(inplace=True)
                ticker_data["ticker"] = ticker
                ticker_data["y"] = ticker_data["adjclose"].shift(-self.projection_horizon)
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                ticker_data = ticker_data[["year","quarter","week","ticker",f"{self.name}_prediction"]]
                sim.append(ticker_data)
            except:
                continue  
        return pd.concat(sim)