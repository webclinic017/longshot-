from strats.astrat import AStrat
from database.adatabase import ADatabase
import numpy as np
import pandas as pd
class EquityRolling(AStrat):
    
    def __init__(self):
        super().__init__("equity_rolling")
        self.db = ADatabase(self.name)
        self.price_db = "prices"
        self.rolling = 30

    def offering_clause(self,date):
        return True
    
    def exit_clause(self,date,position_dictionary):
        return False
    
    def create_sim(self,prices):
        complete = []
        for ticker in prices["ticker"].unique():
            ticker_data = prices[prices["ticker"]==ticker]
            ticker_data["rolling"] = ticker_data["adjclose"].rolling(window=self.rolling).mean()
            ticker_data["delta"] = (ticker_data["rolling"] - ticker_data["adjclose"]) / ticker_data["adjclose"]
            complete.append(ticker_data)
        return pd.concat(complete)
    
    def create_rec(self,today,prices):
        complete = []
        for ticker in prices["ticker"].unique():
            ticker_data = prices[prices["ticker"]==ticker]
            ticker_data["rolling"] = ticker_data["adjclose"].rolling(window=self.rolling).mean()
            ticker_data["delta"] = (ticker_data["rolling"] - ticker_data["adjclose"]) / ticker_data["adjclose"]
            complete.append(ticker_data)    
        final = pd.concat(complete).dropna()
        return final[final["date"]==today].groupby(["date","ticker"]).mean().reset_index()