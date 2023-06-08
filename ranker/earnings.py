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
        self.positions = 10
        
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
                    ticker_data["y"] = ticker_data["earningspersharebasic"].shift(-4)
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

    def price_returns(self,ticker):
        try:
            self.market.connect()
            self.sec.connect()
            cik = int(self.sp500[self.sp500["ticker"]==ticker]["CIK"])
            prices = self.market.retrieve_ticker_prices("prices",ticker)
            prices = p.column_date_processing(prices)
            filing = self.sec.retrieve_filing_data(cik)
            filing = p.column_date_processing(filing)
            self.market.disconnect()
            self.sec.disconnect()  
            financials = filing.groupby(["year","quarter"]).mean().reset_index()
            ticker_sim = prices.copy()
            quarterlies = ticker_sim.groupby(["year","quarter","ticker"]).agg({"adjclose":"first"}).reset_index().rename(columns={"adjclose":"quarter_start"})
            end_quarterlies = ticker_sim.groupby(["year","quarter","ticker"]).agg({"adjclose":"last"}).reset_index().rename(columns={"adjclose":"quarter_end"})
            quarterlies = quarterlies.merge(end_quarterlies[["year","quarter","quarter_end"]],on=["year","quarter"],how="left")
            quarterlies["return_end"] = (quarterlies["quarter_end"] - quarterlies["quarter_start"]) / quarterlies["quarter_start"]
            if "commonstockdividendspersharecashpaid" in financials.columns:
                quarterlies = quarterlies.merge(financials[["year","quarter","commonstockdividendspersharecashpaid"]],how="left")
                quarterlies["commonstockdividendspersharecashpaid"] = quarterlies["commonstockdividendspersharecashpaid"].fillna(0)
            else:
                quarterlies["commonstockdividendspersharecashpaid"] = 0
            quarterly_returns = quarterlies.apply(self.calculate_quarterly_return, axis=1)
            quarterlies.loc[:, "quarterly_return"] = quarterly_returns
            return quarterlies
        except Exception as e:
            print(str(e))
            return pd.DataFrame([{}])
    
    ## helper function for weekly returns
    def calculate_quarterly_return(self,row):
        return row["return_end"] + (row["commonstockdividendspersharecashpaid"] / row["quarter_start"])