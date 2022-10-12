from portfolio.portfolio_utils import PortfolioUtils as portutil
from database.adatabase import ADatabase
import pickle
from datetime import datetime, timedelta
from modeler.modeler import Modeler as m
import pandas as pd

class APortfolio(object):
    
    def __init__(self,strat_class,positions,state):
        self.strat_class = strat_class
        self.positions = positions
        self.state = state
        self.db = ADatabase(f"{self.state.value}_{self.strat_class.name}")
    
    def db_subscribe(self):
        self.db.connect()
    
    def db_unsubscribe(self):
        self.db.disconnect()

    def initialize_backtest_portfolio(self,parameters,initial,date):
        self.portfolio_state = portutil.portfolio_init(parameters,initial,date,self.positions)

    def initialize_portfolio(self,parameters,initial,date):
        portfolios = self.db.retrieve(f"portfolios")
        if portfolios.index.size < 1:
            self.portfolio_state = portutil.portfolio_init(parameters,initial,date,self.positions)
        else:
            portfolios["positions"] = [pickle.loads(x) for x in portfolios["positions"]]
            self.portfolio_state = portfolios[portfolios["date"]<=date].sort_values("date",ascending=False).head(self.positions).to_dict("records")[0]
    
    def pull_trades(self):
        trades = self.db.retrieve(f"trades")
        return trades
    
    def pull_portfolios(self):
        portfolios = self.db.retrieve(f"portfolios")
        return portfolios
    
    def pull_portfolio_max_date(self):
        date_range = self.db.retrieve_collection_date_range(f"portfolios")
        if date_range.index.size < 1:
            max_date = datetime(2022,1,1)
        else:
            max_date = date_range.iloc[-1].item() + timedelta(days=1)
        return max_date
    
    def pull_visualization_max_date(self):
        date_range = self.db.retrieve_collection_date_range(f"visualization")
        if date_range.index.size < 1:
            max_date = datetime(2022,1,1)
        else:
            max_date = date_range.iloc[-1].item() + timedelta(days=1)
        return max_date

    def pull_parameters(self):
        backtest_db = ADatabase(f"backtest_{self.strat_class.name}")
        backtest_db.connect()
        optimal_parameters = backtest_db.retrieve("optimal_parameters")
        backtest_db.disconnect()
        return optimal_parameters.iloc[0]
    
    def transform(self,market,sec):
        market.connect()
        sp500 = market.retrieve("sp500")
        market.disconnect()
        tickers = list(sp500["Symbol"].unique())
        self.db.connect()
        for ticker in tickers:
            try:
                data = self.strat_class.transform(sp500,market,sec,ticker)
                self.db.store("data",data)
            except Exception as e:
                self.db.store("unmodeled",pd.DataFrame([{"ticker":ticker,"error":str(e)}]))
                continue
        self.db.disconnect()
        
    def pull_training_data(self):
        self.db.connect()
        data = self.db.retrieve("data")
        self.db.disconnect()
        return data
    
    def pull_recommend_data(self):
        self.db.connect()
        data = self.db.retrieve("recommend_data")
        self.db.disconnect()
        return data
    
    def pull_recs(self):
        self.db.connect()
        data = self.db.retrieve("recs")
        self.db.disconnect()
        return data
    
    def pull_unmodeled(self):
        self.db.connect()
        data = self.db.retrieve("unmodeled")
        self.db.disconnect()
        return data

    def model(self,start_year,end_year):
        self.db.connect()
        for year in range(start_year,int(end_year)):
            try:
                data = self.pull_training_data()
                training_set = self.strat_class.training_set(data,year)
                training_set = training_set.sample(frac=1)
                prediction_set = self.strat_class.prediction_set(data,year)
                refined = {"X":training_set[self.strat_class.factors],"y":training_set["y"]}
                models = m.regression(refined)
                prediction_set = m.predict(models,prediction_set,self.strat_class.factors)
                prediction_set = self.strat_class.prediction_clean(prediction_set)
                self.db.store("sim",prediction_set)
            except Exception as e:
                print(str(e))
                continue
        self.db.disconnect()
    
    def recommend_transform(self,market,sec):
        market.connect()
        sp500 = market.retrieve("sp500")
        market.disconnect()
        tickers = list(sp500["Symbol"].unique())
        unmodeled = self.pull_unmodeled()
        recs = self.pull_recs()
        if unmodeled.index.size < 1:
            defunct = []
        else:
            defunct = list(unmodeled["ticker"].unique())
        self.db.connect()
        for ticker in tickers:
            if ticker not in defunct:
                try:
                    data = self.strat_class.recommend_transform(recs,sp500,market,sec,ticker)
                    self.db.store("recommend_data",data)
                except Exception as e:
                    continue
        self.db.disconnect()

    def recommend_model(self,start_year,end_year):
        self.db_subscribe()
        for year in range(start_year,int(end_year)):
            try:
                data = self.pull_training_data()
                training_set = self.strat_class.training_set(data,year)
                training_set = training_set.sample(frac=1)
                refined = {"X":training_set[self.strat_class.factors],"y":training_set["y"]}
                models = m.regression(refined)
                models["year"] = year
                models["training_year"] = self.strat_class.training_year
                models["model"] = [pickle.dumps(x) for x in models["model"]]
                self.db.store("models",models)
            except Exception as e:
                print(str(e))
                continue
        self.db_subscribe()
    
    def recommend(self):
        self.db_subscribe()
        models = self.db.retrieve("models")
        recs = self.pull_recs()
        data = self.pull_recommend_data()
        models["model"] = [pickle.loads(x) for x in models["model"]]
        prediction_set = self.strat_class.recommend_set(recs,data)
        prediction_set = m.predict(models,prediction_set,self.strat_class.factors)
        prediction_set = self.strat_class.prediction_clean(prediction_set)
        self.db.store("recs",prediction_set)
        self.db_unsubscribe()