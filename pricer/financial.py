from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.aipricer import AIPricer
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class Financial(AIPricer):

    def __init__(self):
        super().__init__(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_financial"
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
        self.included_columns = ["year","quarter","ticker","adjclose","y"]
        self.included_live_columns = ["year","quarter","ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        
    def training_set(self):
        self.db.connect()
        training_sets = self.db.retrieve("historical_training_set")
        self.db.disconnect()
        self.market.connect()
        self.sec.connect()
        if training_sets.index.size < 1:
            training_set_dfs = []
            for ticker in self.sp500["ticker"].unique()[:10]:
                try:
                    cik = int(self.sp500[self.sp500["ticker"]==ticker]["CIK"])
                    prices = self.market.retrieve_ticker_prices("prices",ticker)
                    prices = p.column_date_processing(prices)
                    prices["year"] = [x.year for x in prices["date"]]
                    prices["quarter"] = [x.quarter for x in prices["date"]]
                    filing = self.sec.retrieve_filing_data(cik)
                    filing = p.column_date_processing(filing)
                    filing = filing.groupby(["year","quarter"]).mean().reset_index()
                    ticker_data = prices.copy()
                    ticker_data.sort_values("date",ascending=True,inplace=True)
                    ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                    ticker_data = ticker_data.groupby(["year","quarter"]).mean().reset_index()
                    ticker_data.dropna(inplace=True)
                    ticker_data["ticker"] = ticker
                    ticker_data["y"] = ticker_data["adjclose"].shift(-4)
                    ticker_data = ticker_data.merge(filing,on=["year","quarter"],how="left").reset_index()
                    ticker_data = ticker_data[self.all_columns]
                    training_set_dfs.append(ticker_data)
                except Exception as e:
                    continue
            self.market.disconnect()
            self.sec.disconnect()  
            training_sets = pd.concat(training_set_dfs)
        self.db.connect()
        self.db.store("historical_training_set",training_sets)
        self.db.disconnect()

    def sim_processor(simulation):
        simulation["week"] = simulation["week"] + 1
        return simulation