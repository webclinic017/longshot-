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
        self.positions = 20
        
    def training_set(self,ticker,prices,filing,current):
        filing = filing.groupby(["year","quarter"]).mean().reset_index()
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year","quarter"]).mean().reset_index()
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        if not current:
            ticker_data["y"] = ticker_data["adjclose"].shift(-4)
            columns = self.all_columns
        else:
            columns = self.all_columns[:-1]
        ticker_data = ticker_data.merge(filing,on=["year","quarter"],how="left").reset_index()
        ticker_data = ticker_data[columns]
        return ticker_data

    def price_returns(self,ticker):
        try:
            self.market.connect()
            self.sec.connect()
            cik = int(self.sp500[self.sp500["ticker"]==ticker]["CIK"])
            prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
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
            quarterlies = quarterlies.sort_values(["year","quarter"])
            if "commonstockdividendspersharecashpaid" in financials.columns:
                quarterlies = quarterlies.merge(financials[["year","quarter","commonstockdividendspersharecashpaid"]],how="left")
                quarterlies["commonstockdividendspersharecashpaid"] = quarterlies["commonstockdividendspersharecashpaid"].fillna(0)
            else:
                quarterlies["commonstockdividendspersharecashpaid"] = 0
            quarterly_returns = quarterlies.apply(self.calculate_quarterly_return, axis=1)
            quarterlies.loc[:, "quarterly_return"] = quarterly_returns
            quarterlies["quarterly_risk_return"]= quarterlies["quarterly_return"].shift(4)
            return quarterlies
        except Exception as e:
            print(str(e))
            return pd.DataFrame([{}])
    
    def risk_returns(self,ticker):
        try:
            self.market.connect()
            self.sec.connect()
            cik = int(self.sp500[self.sp500["ticker"]==ticker]["CIK"])
            prices = self.market.retrieve_ticker_prices(self.asset_class.value,ticker)
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
            quarterlies = quarterlies.sort_values(["year","quarter"])
            if "commonstockdividendspersharecashpaid" in financials.columns:
                quarterlies = quarterlies.merge(financials[["year","quarter","commonstockdividendspersharecashpaid"]],how="left")
                quarterlies["commonstockdividendspersharecashpaid"] = quarterlies["commonstockdividendspersharecashpaid"].fillna(0)
            else:
                quarterlies["commonstockdividendspersharecashpaid"] = 0
            quarterly_returns = quarterlies.apply(self.calculate_quarterly_return, axis=1)
            quarterlies.loc[:, "quarterly_return"] = quarterly_returns
            quarterlies["quarterly_risk_return"]= quarterlies["quarterly_return"].shift(4)
            return quarterlies
        except Exception as e:
            print(str(e))
            return pd.DataFrame([{}])
    
    ## helper function for weekly returns
    def calculate_quarterly_return(self,row):
        return row["return_end"] + (row["commonstockdividendspersharecashpaid"] / row["quarter_start"])