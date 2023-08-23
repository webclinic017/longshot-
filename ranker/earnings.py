from data_product.afinancialstatement_data_product import AFinancialStatementDataProduct
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class Earnings(AFinancialStatementDataProduct):

    def __init__(self):
        super().__init__(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
        self.naming_suffix = "earnings_rank"
        self.lower_bound = 0
        
    def training_set_helper(self,ticker_data,current):
        ticker_data["rank"] = ticker_data["earningspersharebasic"] / ticker_data["adjopen"] 
        return ticker_data