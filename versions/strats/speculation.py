from strats.astrat import AStrat
from database.adatabase import ADatabase
import numpy as np
import pandas as pd
from processor.processor import Processor as p
from datetime import datetime
class Speculation(AStrat):
    
    def __init__(self,training_year):
        super().__init__("speculation")
        self.db = ADatabase(self.name)
        self.training_year = training_year
        self.price_db = "prices"
    
    def offering_clause(self,date):
        return date.weekday() <= 1
    
    def exit_clause(self,date,position_dictionary):
        asset_dictionary = position_dictionary["asset"]
        if len(asset_dictionary.keys()) > 0:
            days_passed = int((date - asset_dictionary["date"]).days)
            max_days = 7 + asset_dictionary["date"].weekday() + 1
            return date.weekday() == 4
        else:
            return False
            
    def daily_rec(self,iterration_sim,date,current_tickers,parameter):
        signal = parameter["signal"]
        todays_recs = iterration_sim[(iterration_sim["date"]==date) & (~iterration_sim["ticker"].isin(current_tickers))]
        todays_recs = todays_recs[(todays_recs["delta"] >= signal)].sort_values("delta",ascending=False)   
        return todays_recs
    
    def transform(self,market,sec):
        market.connect()
        sp500 = market.retrieve("sp500")
        self.db.connect()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        for ticker in sp500["ticker"].unique()[:10]:
            prices = market.retrieve_ticker_prices("prices",ticker)
            prices = p.column_date_processing(prices)
            prices["year"] = [x.year for x in prices["date"]]
            prices["quarter"] = [x.quarter for x in prices["date"]]
            ticker_data = prices[prices["ticker"]==ticker]
            ticker_data.sort_values("date",ascending=True,inplace=True)
            ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
            ticker_data = ticker_data.groupby(["year","week"]).mean().reset_index()
            for i in range(14):
                ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
            ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
            ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
            ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
            ticker_data.dropna(inplace=True)
            ticker_data["ticker"] = ticker
            ticker_data["y"] = ticker_data["adjclose"].shift(-1)
            ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
            ticker_data.dropna(inplace=True)
            ticker_data = ticker_data.merge(sp500[["ticker","GICS Sector","GICS Sub-Industry"]],on="ticker",how="left")[self.included_columns]
            self.db.store("data",ticker_data)
        market.disconnect()
    
    def recommend_transform(self,market,sec):
        market.connect()
        sp500 = market.retrieve("sp500")
        self.db.connect()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        recs = self.pull_recommend_data()
        if recs.index.size < 1:
            if "year" not in recs.columns:
                start_year = datetime.now().year - 1
                start_week = 0
            else:
                start_year = recs["year"].max().year
                start_week = recs["week"].max() + 1
            for ticker in sp500["ticker"].unique()[:10]:
                prices = market.retrieve_ticker_prices("prices",ticker)
                prices = p.column_date_processing(prices)
                prices["year"] = [x.year for x in prices["date"]]
                prices["quarter"] = [x.quarter for x in prices["date"]]
                prices["week"] = [x.week for x in prices["date"]]
                ticker_data = prices[prices["ticker"]==ticker]
                ticker_data.sort_values("date",ascending=True,inplace=True)
                ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
                ticker_data = ticker_data.groupby(["year","week"]).mean().reset_index()
                for i in range(14):
                    ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
                ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
                ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
                ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
                ticker_data.dropna(inplace=True)
                ticker_data["ticker"] = ticker
                ticker_data["y"] = ticker_data["adjclose"].shift(-1)
                ticker_data = ticker_data.replace([np.inf, -np.inf], np.nan).dropna()
                ticker_data.dropna(inplace=True)
                ticker_data = ticker_data.merge(sp500[["ticker","GICS Sector","GICS Sub-Industry"]],on="ticker",how="left")[self.included_columns]
                ticker_data = ticker_data[(ticker_data["year"]==start_year) & (ticker_data["week"]>=start_week)]
                self.db.store("recommend_data",ticker_data)
        market.disconnect()
    
    def training_set(self,filings,year):
        return filings[(filings["year"]>=year-self.training_year) & (filings["year"]<year)][self.included_columns].reset_index(drop=True)
    
    def prediction_set(self,filings,year):
        return filings[(filings["year"]==year)].reset_index(drop=True)
    
    def prediction_clean(self,prediction_set):
        prediction_set["week"] = prediction_set["week"] + 1
        prediction_set["training_year"] = self.training_year
        included_cols = [x for x in prediction_set.columns if "prediction" in x or "score" in x]
        included_cols.extend(["year","week","ticker","training_year"])
        return prediction_set[included_cols]
    
    def create_sim(self,prices,simulation):
        simulation["year"] = [int(x) for x in simulation["year"]]
        sim = prices.copy().merge(simulation[(simulation["training_year"]==self.training_year)],on=["year","week","ticker"],how="left")
        sim = sim.dropna()
        sim["prediction"] = (sim["xgb_prediction"] + sim["skl_prediction"] + sim["cat_prediction"]) / 3
        sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["model_delta"] = sim["prediction"].pct_change()
        return sim    
    
    def create_rec(self,today,prices):
        self.db.connect()
        simulation = self.db.retrieve(f"recs")
        self.db.disconnect()
        sim = prices.copy().merge(simulation[(simulation["training_year"]==self.training_year)],on=["year","week","ticker"],how="left")
        sim = sim.dropna()
        sim["prediction"] = (sim["xgb_prediction"] + sim["skl_prediction"] + sim["cat_prediction"]) / 3
        sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["model_delta"] = sim["prediction"].pct_change()
        return sim[sim["date"]==today].groupby(["date","ticker"]).mean().reset_index()