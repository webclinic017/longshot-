from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from strategy.aistrategy import AIStrategy

class Speculation(AIStrategy):

    def __init__(self,asset_class,quarterly,classification):
        super().__init__(asset_class,quarterly,classification)
        self.name = f"{self.group_timeframe}ly_{asset_class}_speculation"
        self.db = ADatabase(self.name) if quarterly else ADatabase(self.name)
        self.factors = [str(x) for x in range(14)]
        self.classification_factors = ["d1","d2","d3","rolling14"]
        self.included_columns = ["year",self.group_timeframe,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.group_timeframe,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.all_classification_columns = self.classification_factors + self.included_columns
        self.positions = 10 if asset_class == "prices" else 1
        
    def training_set(self,market,sec,sp500):
        training_sets = []
        tickers = ["BTC"] if self.asset_class == "crypto" else list(sp500["ticker"].unique()[:10])
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
    
    def classification_training_set(self,market,sec,sp500):
        training_sets = []
        tickers = ["BTC"] if self.asset_class == "crypto" else list(sp500["ticker"].unique()[:10])
        for ticker in tickers:
            try:
                prices = market.retrieve_ticker_prices(self.asset_class,ticker)
                prices = p.column_date_processing(prices)
                ticker_data = prices
                ticker_data.sort_values("date",ascending=True,inplace=True)
                ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                ticker_data = ticker_data.groupby(["year","week"]).mean().reset_index()
                ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
                ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
                ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
                ticker_data["rolling14"] = ticker_data["adjclose"].rolling(window=14).mean()
                ticker_data.dropna(inplace=True)
                ticker_data["ticker"] = ticker
                ticker_data["future"] = ticker_data["adjclose"].shift(-1)
                ticker_data["delta"] = (ticker_data["future"] - ticker_data["adjclose"]) / ticker_data["adjclose"]
                ticker_data["y"] = [x > 0 for x in ticker_data["delta"]]
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                ticker_data = ticker_data[self.all_classification_columns]
                training_sets.append(ticker_data)
            except:
                continue  
        return pd.concat(training_sets)
    
    def create_sim(self,simulation,price_returns):
        sim = price_returns.merge(self.tyields[["year","week","weekly_yield"]],on=["year","week"],how="left")
        colcol = [x for x in simulation.columns if self.strat_class.name in x] + ["year","week","ticker"]
        sim = sim.merge(simulation[colcol],on=["year","week","ticker"],how="left")
        classification_col = [x for x in sim.columns if "classification_prediction" in x][0]
        sim = sim.dropna().groupby(["year","week","date","ticker",classification_col]).mean().reset_index()
        return sim
    
    @classmethod
    def transform(self,ticker_data):
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year","week"]).mean().reset_index()
        for i in range(14):
            ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
        return ticker_data