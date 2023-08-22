from database.sec import SEC
from data_product.anonai_data_product import ANonAIDataProduct
from processor.processor import Processor as p
import numpy as np
import pandas as pd
# description: class for data products
class AFinancialStatementDataProduct(ANonAIDataProduct):
    
    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.sec = SEC()
    
    def training_set(self):
        training_sets = []
        self.market.connect()
        self.sec.connect()
        for ticker in self.sp100["ticker"].unique():
            try:
                cik = self.sp100[self.sp100["ticker"]==ticker]["CIK"]
                filings = self.sec.retrieve_filing_data(int(cik))
                filings["date"] = pd.to_datetime(filings["filed"],format="%Y%m%d")
                filings = p.column_date_processing(filings)
                prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
                ticker_data = p.column_date_processing(prices)
                ticker_data.sort_values("date",inplace=True)
                ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                filing = filings.groupby(["year","quarter"]).mean().reset_index()
                filing["year"] = [row[1]["year"] if row[1]["quarter"] != 4 else row[1]["year"]+1 for row in filing.iterrows()]
                filing["quarter"] = [row[1]["quarter"]+1 if row[1]["quarter"] != 4 else 1 for row in filing.iterrows()]
                if "dividendscommonstockcash" not in filing.columns:
                    filing["dividendscommonstockcash"] = 0
                if "weightedaveragenumberofsharesoutstandingbasic" not in filing.columns:
                    filing["weightedaveragenumberofsharesoutstandingbasic"] = 0
                if "earningspersharebasic" not in filing.columns:
                    filing["earningspersharebasic"] = 0
                filing["dividend"] = filing["dividendscommonstockcash"] / filing["weightedaveragenumberofsharesoutstandingbasic"]
                ticker_data = ticker_data.merge(filing[["year","quarter","dividend","earningspersharebasic"]],on=["year","quarter"],how="left").reset_index()
                ticker_data = self.training_set_helper(ticker_data,False)
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                training_sets.append(ticker_data)
            except Exception as e:
                print(str(e))
                continue
        self.market.disconnect()
        self.sec.disconnect()
        data = pd.concat(training_sets)
        self.training_data = data