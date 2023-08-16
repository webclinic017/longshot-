import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from data_product.anonai_data_product import NonAIDataProduct

## class to store a pricing strategy revolved around rolling average prices from the past
class Rolling(NonAIDataProduct):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_rolling_ranker"
        self.db = ADatabase(self.name)
        
    def training_set_helper(self,ticker,prices,current):
        ticker_data = prices[prices["ticker"]==ticker]
        ticker_data.sort_values("date",inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data[f"rank"] = (ticker_data["adjclose"].rolling(self.time_horizon_class.window).mean() - ticker_data["adjclose"]) / ticker_data["adjclose"]
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        return ticker_data
    
    # def backtest_rank(self,simulation):
    #     simulation["projected_pe"] =  (simulation["rank_prediction"] - simulation["adjclose"]) / simulation["adjclose"]
    #     industry_filter = []
    #     for industry in self.sp500["GICS Sector"].unique():
    #         tickers = list(self.sp500[self.sp500["GICS Sector"]==industry]["ticker"])
    #         industry_simulation = simulation[simulation["ticker"].isin(tickers)]
    #         filtered = industry_simulation[(industry_simulation["projected_pe"] <= industry_simulation["projected_pe"].mean()) & (industry_simulation["projected_pe"] > 0)]
    #         industry_filter.append(filtered)
    #     simulation = pd.concat(industry_filter)
    #     return simulation