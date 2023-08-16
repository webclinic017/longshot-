from data_product.adata_product import ADataProduct
from tqdm import tqdm
import pandas as pd
from processor.processor import Processor as p

class ARanker(ADataProduct):
    
    def __init__(self,asset_class,time_horizon):
        super.__init__(self,asset_class,time_horizon)

    def training_set(self):
        self.market.connect()
        self.sec.connect()
        training_sets = []
        for ticker in tqdm(self.sp500["ticker"].unique()):
            try:
                cik = self.sp500[self.sp500["ticker"]==ticker]["CIK"]
                prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
                filings = self.sec.retrieve_filing_data(int(cik))
                prices = p.column_date_processing(prices)
                filings = p.column_date_processing(filings)
                ticker_data = self.training_set_helper(ticker,prices,filings,False)
                training_sets.append(ticker_data)
            except Exception as e:
                print(str(e))
                continue
        self.market.disconnect()
        self.sec.disconnect()
        data = pd.concat(training_sets)
        training_data = data.dropna().copy().sort_values(["year",self.time_horizon_class.naming_convention])
        self.training_data = training_data