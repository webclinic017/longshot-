from strats.astrat import AStrat
from database.adatabase import ADatabase
from datetime import datetime
from processor.processor import Processor as p
import pandas as pd
class Financial(AStrat):
    
    def __init__(self,training_year):
        super().__init__("financial")
        self.db = ADatabase(self.name)
        self.training_year = training_year
        self.price_db = "prices"

    def offering_clause(self,date):
        return date.weekday() <= 1
    
    def exit_clause(self,date,position_dictionary):
        asset_dictionary = position_dictionary["asset"]
        if len(asset_dictionary.keys()) > 0:
            purchase_quarter = (asset_dictionary["date"].month-1) // 3 + 1
            current_quarter = (date.month-1) // 3 + 1
            return current_quarter != purchase_quarter
        else:
            return False
    
    def daily_rec(self,iterration_sim,date,current_tickers,parameter):
        signal = parameter["signal"]
        todays_recs = iterration_sim[(iterration_sim["date"]==date) & (~iterration_sim["ticker"].isin(current_tickers))]
        todays_recs = todays_recs[(todays_recs["delta"] >= signal)].sort_values("delta",ascending=False)   
        return todays_recs

    def transform(self,market,sec):
        data = self.pull_training_data()
        market.connect()
        sp500 = market.retrieve("sp500")
        market.disconnect()
        self.db.connect()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        if data.index.size < 1:
            for ticker in sp500["ticker"].unique():
                try:
                    row = sp500[sp500["ticker"]==ticker]
                    cik = int(row["CIK"])
                    sec.connect()
                    filing_data = sec.retrieve_filing_data(cik)
                    sec.disconnect()
                    filing_data["date"] = [datetime.strptime(str(x),"%Y%m%d") for x in filing_data["filed"]]
                    filing_data = p.column_date_processing(filing_data)
                    filing_data["ticker"] = ticker
                    market.connect()
                    prices = market.retrieve_ticker_prices("prices",ticker)
                    market.disconnect()
                    prices = p.column_date_processing(prices)
                    prices.sort_values("date",inplace=True)
                    prices = prices.groupby(["year","quarter"]).mean().reset_index()
                    filing_data = filing_data.merge(prices,on=["year","quarter"],how="left")
                    filing_data["y"] = filing_data['adjclose'].shift(-1)
                    complete = filing_data.fillna(method="bfill").fillna(method="ffill").groupby(["year","quarter","ticker"]).mean().reset_index()
                    complete = complete.merge(sp500[["ticker","GICS Sector","GICS Sub-Industry"]],on="ticker",how="left")[self.included_columns]
                    self.db.store("data",complete)
                except Exception as e:
                    self.db.store("unmodeled",pd.DataFrame([{"ticker":ticker,"error":str(e)}]))
        self.db.disconnect()
    
    def recommend_transform(self,market,sec):
        unmodeled = self.pull_unmodeled()
        recs = self.pull_recommend_data()
        market.connect()
        sp500 = market.retrieve("sp500")
        market.disconnect()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        if recs.index.size < 1:
            for ticker in sp500["ticker"].unique():
                try:
                    if ticker not in unmodeled["ticker"].unique():
                        if "year" not in recs.columns:
                            start_year = datetime.now().year - 1
                            start_quarter = 4
                        else:
                            start_year = recs["year"].max().year
                            start_quarter = recs["quarter"].max() + 1
                        row = sp500[sp500["ticker"]==ticker]
                        cik = int(row["CIK"])
                        sec.connect()
                        filing_data = sec.retrieve_filing_data(cik)
                        sec.disconnect()
                        filing_data["date"] = [datetime.strptime(str(x),"%Y%m%d") for x in filing_data["filed"]]
                        filing_data = p.column_date_processing(filing_data)
                        filing_data["ticker"] = ticker
                        market.connect()
                        prices = market.retrieve_ticker_prices("prices",ticker)
                        market.disconnect()
                        prices = p.column_date_processing(prices)
                        prices.sort_values("date",inplace=True)
                        prices = prices.groupby(["year","quarter"]).mean().reset_index()
                        filing_data = filing_data.merge(prices,on=["year","quarter"],how="left")
                        filing_data.reset_index(inplace=True,drop=True)
                        starting_index = filing_data[(filing_data["year"]==start_year) & (filing_data["quarter"]==start_quarter)].index.values[0]
                        filing_data = filing_data.iloc[starting_index:]
                        complete = filing_data.fillna(method="bfill").fillna(method="ffill").groupby(["year","quarter","ticker"]).mean().reset_index()
                        complete = complete.merge(sp500[["ticker","GICS Sector","GICS Sub-Industry"]],on="ticker",how="left")[[x for x in self.included_columns if x != "y"]]
                        self.db.store("recommend_data",complete)
                except:
                    continue

    def training_set(self,filings,year):
        return filings[(filings["year"]>=year-self.training_year) & (filings["year"]<year)][self.included_columns].reset_index(drop=True)
    
    def prediction_set(self,filings,year):
        return filings[(filings["year"]==year)].reset_index(drop=True)
    
    def recommend_set(self,model_type):
        filing_data = self.pull_recommend_data()
        recs = self.pull_recs(model_type)
        start_quarter = 4
        start_year = 2022
        # if "year" not in recs.columns:
        #     start_year = datetime.now().year - 1
        #     start_quarter = 4   
        # else:
        #     start_year = recs["year"].max()
        #     if recs["quarter"].max() == 4:
        #         start_quarter = 1
        #         start_year = recs["year"].max() + 1
        #     else:
        #         start_quarter = recs["quarter"].max() + 1
        # print(filing_data["year"].max(), filing_data["quarter"].max())
        filing_data.reset_index(inplace=True,drop=True)
        starting_index = filing_data[(filing_data["year"]==start_year) & (filing_data["quarter"]==start_quarter)].index.values[0]
        filing_data = filing_data.iloc[starting_index:].dropna()
        return filing_data
    
    def prediction_clean(self,prediction_set):
        prediction_set["new_quarter"] = [(row[1]["quarter"] + 1) % 4 for row in prediction_set.iterrows()]
        prediction_set["new_quarter"] = prediction_set["new_quarter"].replace(0,4)
        prediction_set["new_year"] = [row[1]["year"] if row[1]["quarter"] + 1 <=4 else row[1]["year"] + 1 for row in prediction_set.iterrows()]
        prediction_set["year"] = prediction_set["new_year"]
        prediction_set["quarter"] = prediction_set["new_quarter"]
        prediction_set["training_year"] = self.training_year
        included_cols = [x for x in prediction_set.columns if "prediction" in x or "score" in x]
        included_cols.extend(["year","quarter","ticker","training_year"])
        return prediction_set[included_cols]
    
    def create_sim(self,prices,simulation):
        sim = prices.copy().merge(simulation,on=["year","quarter","ticker"],how="left")
        print(sim.columns)
        sim = sim.dropna()
        sim = sim.groupby(["year","date","ticker","GICS Sector","GICS Sub-Industry"]).mean().reset_index()
        sim["prediction"] = (sim["xgb_prediction"] + sim["skl_prediction"] + sim["cat_prediction"]) / 3
        sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["model_delta"] = sim["prediction"].pct_change()
        return sim
    
    def create_rec(self,simulation,today,prices):
        year = today.year
        quarter =  (today.month -1)//3 + 1
        simulation["year"] = [int(x) for x in simulation["year"]]
        sim = prices.copy().merge(simulation[(simulation["training_year"]==self.training_year) & (simulation["quarter"]==quarter) \
                     & (simulation["year"]==year)],on=["year","quarter","ticker"],how="left")
        sim = sim.dropna()
        sim["prediction"] = (sim["xgb_prediction"] + sim["skl_prediction"] + sim["cat_prediction"]) / 3
        sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["model_delta"] = sim["prediction"].pct_change()
        return sim[sim["date"]==today].groupby(["date","ticker","GICS Sector","GICS Sub-Industry"]).mean().reset_index()