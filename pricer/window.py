import numpy as np
from data_product.anonai_data_product import ANonAIDataProduct

## class to store a pricing strategy revolved around window average prices from the past
class Window(ANonAIDataProduct):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "window_price"
    
    def training_set_helper(self,ticker_data,current):
        ticker_data[f"price"] = ticker_data["adjclose"].shift(self.time_horizon_class.window)
        return ticker_data