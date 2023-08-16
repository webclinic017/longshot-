import numpy as np
from data_product.anonai_data_product import NonAIDataProduct

## class to store a pricing strategy revolved around window average prices from the past
class Window(NonAIDataProduct):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "window"
    
    def training_set_helper(self,ticker,prices,current):
        ticker_data = prices[prices["ticker"]==ticker]
        ticker_data.sort_values("date",inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data[f"price"] = ticker_data["adjclose"].shift(self.time_horizon_class.window)
        ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
        ticker_data.dropna(inplace=True)
        return ticker_data