from time_horizons.time_horizons_factory import TimeHorizonFactory
from database.market import Market
from database.sec import SEC
from data_product.anonai_data_product import ANonAIDataProduct
from processor.processor import Processor as p
import numpy as np
import pandas as pd
# description: class for data products
class AFinancialStatementDataProduct(ANonAIDataProduct):
    
    def __init__(self,asset_class,time_horizon):
        self.time_horizon_class = TimeHorizonFactory.build(time_horizon)
        self.asset_class = asset_class
        self.market = Market()
        self.sec = SEC()
        self.pull_sp500()
    
    def training_set(self):
        training_sets = []
        self.market.connect()
        self.sec.connect()
        for ticker in self.sp500["ticker"].unique():
            try:
                cik = self.sp500[self.sp500["ticker"]==ticker]["CIK"]
                filings = self.sec.retrieve_filing_data(int(cik))
                filings = p.column_date_processing(filings)
                prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
                ticker_data = p.column_date_processing(prices)
                ticker_data.sort_values("date",inplace=True)
                ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                ticker_data = self.training_set_helper(ticker_data,filings,False)
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                training_sets.append(ticker_data)
            except Exception as e:
                print(str(e))
                continue
        self.market.disconnect()
        self.sec.disconnect()
        data = pd.concat(training_sets)
        training_data = data.dropna().copy().sort_values(["year",self.time_horizon_class.naming_convention])
        self.training_data = training_data