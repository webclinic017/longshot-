# import numpy as np
# from pricer.nonaipricer import NonAIPricer

# ## strategy is revolved around the difference between rolling 20 and rolling 5 day averages
# class MeanReversion(NonAIPricer):
    
#     def __init__(self,asset_class,time_horizon):
#         super().__init__(asset_class,time_horizon)
#         self.naming_suffix = "mean_reversion"

#     def training_set_helper(self,ticker,prices,current):
#         ticker_data = prices[prices["ticker"]==ticker]
#         ticker_data.sort_values("date",ascending=True,inplace=True)
#         ticker_data["adjopen"] = [float(x) for x in ticker_data["adjopen"]]
#         ticker_data = ticker_data.groupby(["year",self.time_horizon_class.naming_convention,"ticker"]).mean().reset_index()
#         ticker_data[f"rolling20"] = ticker_data["adjopen"].rolling(20).mean()
#         ticker_data[f"rolling5"] = ticker_data["adjopen"].rolling(5).mean()
#         ticker_data[f"price_prediction"] = (ticker_data["rolling5"] - ticker_data["rolling20"]) / ticker_data["rolling20"]
#         ticker_data.dropna(inplace=True)
#         ticker_data["ticker"] = ticker
#         ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
#         ticker_data.dropna(inplace=True)
#         ticker_data = ticker_data[["year",self.time_horizon_class.naming_convention,"ticker",f"price_prediction"]]
#         return ticker_data