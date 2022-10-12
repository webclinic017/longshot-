from strats.astrat import AStrat
from database.adatabase import ADatabase
import numpy as np
import pandas as pd
class BTCBrute(AStrat):
    
    def __init__(self):
        super().__init__("bitcoin_brute")
        self.db = ADatabase(self.name)
        self.price_db = "crypto"
        self.rolling = 30

    def offering_clause(self,date):
        return True
    
    def exit_clause(self,date,position_dictionary):
        return False
    
    def create_sim(self,prices):
        prices["rolling"] = prices["adjclose"].rolling(window=self.rolling).mean()
        prices["delta"] = (prices["rolling"] - prices["adjclose"]) / prices["adjclose"]
        return prices
    
    def create_rec(self,today,prices):
        prices["rolling"] = prices["adjclose"].rolling(window=self.rolling).mean()
        prices["delta"] = (prices["rolling"] - prices["adjclose"]) / prices["adjclose"]
        return prices[prices["date"]==today].groupby(["date","ticker"]).mean().reset_index()