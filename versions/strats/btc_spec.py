from strats.astrat import AStrat
from database.adatabase import ADatabase
import numpy as np
import pandas as pd
class BTCSpec(AStrat):
    
    def __init__(self,training_year):
        super().__init__("bitcoin_speculation")
        self.db = ADatabase(self.name)
        self.training_year = training_year
        self.price_db = "crypto"
        self.projection_value = 5

    def offering_clause(self,date):
        return date.weekday() <= 1
    
    def exit_clause(self,date,position_dictionary):
        asset_dictionary = position_dictionary["asset"]
        if len(asset_dictionary.keys()) > 0:
            days_passed = int((date - asset_dictionary["date"]).days)
            return days_passed >= 7
        else:
            return False

    def transform(self,ticker_data,ticker):
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        for i in range(14):
            ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
        ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
        ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
        ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        ticker_data["y"] = ticker_data["adjclose"].shift(-self.projection_value)
        ticker_data.replace([np.inf, -np.inf], np.nan, inplace=True)
        ticker_data.dropna(inplace=True)
        return ticker_data
    
    def transform_live(self,ticker_data,ticker):
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        for i in range(14):
            ticker_data[str(i)] = ticker_data["adjclose"].shift(i)
        ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
        ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
        ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
        ticker_data.dropna(inplace=True)
        ticker_data["ticker"] = ticker
        ticker_data.replace([np.inf, -np.inf], np.nan, inplace=True)
        ticker_data.dropna(inplace=True)
        return ticker_data
    
    def training_set(self,filings,year):
        return filings[(filings["year"]>=year-self.training_year) & (filings["year"]<year)][self.included_columns].reset_index(drop=True)
    
    def prediction_set(self,filings,year):
        return filings[(filings["year"]==year)].reset_index(drop=True)
    
    def prediction_clean(self,prediction_set):
        prediction_set["training_year"] = self.training_year
        included_cols = [x for x in prediction_set.columns if "prediction" in x or "score" in x]
        included_cols.extend(["date","ticker","training_year"])
        return prediction_set[included_cols]
    
    def create_sim(self,prices):
        self.db.connect()
        simulation = self.db.retrieve(f"sim")
        self.db.disconnect()
        sim = prices.copy().merge(simulation[(simulation["training_year"]==self.training_year)],on=["date","ticker"],how="left")
        sim = sim.dropna()
        sim["prediction"] = (sim["xgb_prediction"] + sim["skl_prediction"] + sim["cat_prediction"]) / 3
        sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["model_delta"] = sim["prediction"].pct_change()
        return sim
    
    def create_rec(self,today,prices):
        self.db.connect()
        simulation = self.db.retrieve(f"recs")
        self.db.disconnect()
        sim = prices.copy().merge(simulation[(simulation["training_year"]==self.training_year)],on=["date","ticker"],how="left")
        sim = sim.dropna()
        sim["prediction"] = (sim["xgb_prediction"] + sim["skl_prediction"] + sim["cat_prediction"]) / 3
        sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["model_delta"] = sim["prediction"].pct_change()
        return sim[sim["date"]==today].groupby(["date","ticker"]).mean().reset_index()