from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from ranker.nonairanker import NonAIRanker

class Rolling(NonAIRanker):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_rolling_ranker"
        self.db = ADatabase(self.name)
        self.factors = []
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        
    def training_set(self,ticker,prices):
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data[f"rank_prediction"] = ticker_data["adjclose"].rolling(100).mean()
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        ticker_data["y"] = ticker_data["adjclose"].shift(-self.time_horizon_class.y_pivot_number)
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        ticker_data = ticker_data[["year","quarter","week","ticker",f"rank_prediction"]]
        return ticker_data
    
    def backtest_rank(self,simulation):
        simulation["projected_pe"] =  (simulation["rank_prediction"] - simulation["adjclose"]) / simulation["adjclose"]
        industry_filter = []
        for industry in self.sp500["GICS Sector"].unique():
            tickers = list(self.sp500[self.sp500["GICS Sector"]==industry]["ticker"])
            industry_simulation = simulation[simulation["ticker"].isin(tickers)]
            filtered = industry_simulation[(industry_simulation["projected_pe"] <= industry_simulation["projected_pe"].mean()) & (industry_simulation["projected_pe"] > 0)]
            industry_filter.append(filtered)
        simulation = pd.concat(industry_filter)
        return simulation