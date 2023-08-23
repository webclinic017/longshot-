from data_product.anonai_data_product import ANonAIDataProduct
## class to store a classification strategy revolved around rolling average prices from the past
class Rolling(ANonAIDataProduct):
    
    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.naming_suffix = "rolling_classification"

    def training_set_helper(self,ticker_data,current):
        ticker_data[f"classification"] = (ticker_data["adjopen"].shift(self.time_horizon_class.window) - ticker_data["adjopen"]) > 0
        return ticker_data