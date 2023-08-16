from time_horizons.time_horizons_factory import TimeHorizonFactory
from database.market import Market
from database.adatabase import ADatabase
from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
from tqdm import tqdm
from data_product.adata_product import ADataProduct

# description: class for pricing strategies
class APricer(ADataProduct):
    
    def __init__(self,asset_class,time_horizon):
        super.__init__(self,asset_class,time_horizon)
    
    def training_set(self):
        self.market.connect()
        training_sets = []
        for ticker in tqdm(self.sp500["ticker"].unique()):
            try:
                prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
                prices = p.column_date_processing(prices)
                ticker_data = self.training_set_helper(ticker,prices,False)
                training_sets.append(ticker_data)
            except Exception as e:
                print(str(e))
                continue
        self.market.disconnect()
        data = pd.concat(training_sets)
        training_data = data.dropna().copy().sort_values(["year",self.time_horizon_class.naming_convention])
        self.training_data = training_data