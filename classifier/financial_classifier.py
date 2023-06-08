from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from classifier.aiclassifier import AIClassifier


class FinancialClassifier(AIClassifier):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_financial_classification"
        self.db = ADatabase(self.name)
        self.factors = [
                            'assets',
                            'liabilitiesandstockholdersequity',
                            'incometaxexpensebenefit',
                            'retainedearningsaccumulateddeficit',
                            'accumulatedothercomprehensiveincomelossnetoftax',
                            'earningspersharebasic',
                            'earningspersharediluted',
                            'propertyplantandequipmentnet',
                            'cashandcashequivalentsatcarryingvalue',
                            'entitycommonstocksharesoutstanding',
                            'weightedaveragenumberofdilutedsharesoutstanding',
                            'weightedaveragenumberofsharesoutstandingbasic',
                            'stockholdersequity'
                        ]
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 10 if asset_class.value == "stocks" else 1
        
    def training_set(self):
        self.db.connect()
        training_sets = self.db.retrieve("historical_training_set")
        self.db.disconnect()
        tickers = ["BTC"] if self.asset_class.value == "crypto" else list(self.sp500["ticker"].unique()[:10])
        if training_sets.index.size < 1:
            training_set_dfs = []
            self.market.connect()
            self.sec.connect()
            for ticker in tickers:
                try:
                    cik = int(self.sp500[self.sp500["ticker"]==ticker]["CIK"])
                    prices = self.market.retrieve_ticker_prices("prices",ticker)
                    prices = p.column_date_processing(prices)
                    filing = self.sec.retrieve_filing_data(cik)
                    filing = p.column_date_processing(filing)
                    filing = filing.groupby(["year","quarter"]).mean().reset_index()
                    ticker_data = prices.copy()
                    ticker_data.sort_values("date",ascending=True,inplace=True)
                    ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                    ticker_data = ticker_data.groupby(["year","quarter"]).mean().reset_index()
                    ticker_data.dropna(inplace=True)
                    ticker_data["ticker"] = ticker
                    ticker_data["future"] = ticker_data["adjclose"].shift(-4)
                    ticker_data["delta"] = (ticker_data["future"] - ticker_data["adjclose"]) / ticker_data["adjclose"]
                    ticker_data["y"] = [x > 0 for x in ticker_data["delta"]]
                    ticker_data = ticker_data.merge(filing,on=["year","quarter"],how="left").reset_index()
                    ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                    ticker_data = ticker_data[self.all_columns]
                    training_set_dfs.append(ticker_data)
                except Exception as e:
                    print(str(e))
                    continue  
            self.market.disconnect()
            self.sec.disconnect() 
            training_sets = pd.concat(training_set_dfs)
            self.db.connect()
            self.db.store("historical_training_set",training_sets)
            self.db.disconnect()