# import numpy as np
# from pricer.nonaipricer import NonAIPricer


# ## strategy revolved around applying standard deviation in pricing derived from rolling averages
# class DevRolling(NonAIPricer):
    
#     def __init__(self,asset_class,time_horizon):
#         super().__init__(asset_class,time_horizon)
#         self.naming_suffix = "dev_rolling"

#     def training_set_helper(self,ticker,prices,current):
#         ticker_data = prices[prices["ticker"]==ticker]
#         ticker_data.sort_values("date",ascending=True,inplace=True)
#         ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
#         ticker_data = ticker_data.groupby(["year",self.time_horizon_class.naming_convention,"ticker"]).mean().reset_index()
#         ticker_data[f"rolling3"] = ticker_data["adjclose"].rolling(3).mean()
#         ticker_data[f"rollingstd"] = ticker_data["adjclose"].rolling(3).std()
#         ticker_data[f"price_prediction"] = ticker_data["rolling3"] + 2 * ticker_data["rollingstd"]
#         ticker_data.dropna(inplace=True)
#         ticker_data["ticker"] = ticker
#         ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
#         ticker_data.dropna(inplace=True)
#         ticker_data = ticker_data[["year",self.time_horizon_class.naming_convention,"ticker",f"price_prediction"]]
#         return ticker_data