# import numpy as np
# from pricer.nonaipricer import NonAIPricer


# ## strategy revolved around pricing based on price levels above the max price
# class DailyBreakout(NonAIPricer):
    
#     def __init__(self,asset_class,time_horizon):
#         super().__init__(asset_class,time_horizon)
#         self.naming_suffix = "breakout"

#     def training_set_helper(self,ticker,prices,current):
#         ticker_data = prices[prices["ticker"]==ticker]
#         ticker_data.sort_values("date",ascending=True,inplace=True)
#         ticker_data["adjopen"] = [float(x) for x in ticker_data["adjopen"]]
#         ticker_data = ticker_data.groupby(["year",self.time_horizon_class.naming_convention,"ticker"]).mean().reset_index()
#         ticker_data[f"price_prediction"] = ticker_data["adjopen"].rolling(self.time_horizon_class.rolling).max()
#         ticker_data.dropna(inplace=True)
#         ticker_data["ticker"] = ticker
#         ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
#         ticker_data.dropna(inplace=True)
#         ticker_data = ticker_data[["year",self.time_horizon_class.naming_convention,"ticker",f"price_prediction"]]
#         return ticker_data