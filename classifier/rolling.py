import numpy as np
from classifier.nonaiclassifier import NonAIClassifier

## class to store a pricing strategy revolved around rolling average prices from the past
class Rolling(NonAIClassifier):
    
    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "rolling_classification"

    def training_set_helper(self,ticker,prices,current):
        ticker_data = prices[prices["ticker"]==ticker]
        ticker_data.sort_values("date",inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data[f"classification"] = ticker_data["adjclose"].shift(self.time_horizon_class.window)
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        return ticker_data