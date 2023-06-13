from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from ranker.airanker import AIRanker
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class Earnings(AIRanker):

    def __init__(self):
        super().__init__(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_earnings_rank"
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
        self.positions = 20
        
    def training_set(self,ticker,prices,filing,current):
        filing = filing.groupby(["year","quarter"]).mean().reset_index()
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year","quarter"]).mean().reset_index()
        ticker_data["ticker"] = ticker
        ticker_data = ticker_data.merge(filing,on=["year","quarter"],how="left").reset_index()
        if not current:
            ticker_data["y"] = ticker_data["earningspersharebasic"].shift(-4)
            columns = self.all_columns
        else:
            columns = self.all_columns[:-1]
        ticker_data = ticker_data[columns].dropna()
        return ticker_data

    def backtest_rank(self,simulation):
        simulation["projected_pe"] = simulation["price_prediction"] / simulation["rank_prediction"]
        industry_filter = []
        for industry in self.sp500["GICS Sector"].unique():
            tickers = list(self.sp500[self.sp500["GICS Sector"]==industry]["ticker"])
            industry_simulation = simulation[simulation["ticker"].isin(tickers)]
            filtered = industry_simulation[(industry_simulation["projected_pe"] <= industry_simulation["projected_pe"].mean()) & (industry_simulation["projected_pe"] > 0)]
            industry_filter.append(filtered)
        simulation = pd.concat(industry_filter)
        return simulation