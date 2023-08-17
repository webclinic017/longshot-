import numpy as np
from data_product.anonai_data_product import ANonAIDataProduct

## class to store a pricing strategy revolved around rolling average prices from the past
class Rolling(ANonAIDataProduct):
    
    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "rolling_price"

    def training_set_helper(self,ticker_data,current):
        ticker_data[f"price"] = ticker_data["adjclose"].rolling(self.time_horizon_class.window).mean()
        return ticker_data