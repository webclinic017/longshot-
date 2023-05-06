import pandas as pd
from strats.istrat import IStrat
import pickle
from portfolio.portfolio_utils import PortfolioUtils as portutils
class AStrat(IStrat):
    def __init__(self,name):
        self.name = name
    
    def check_factors(self):
        self.db.connect()
        factors_init = "factors" in self.db.collection_names()
        self.db.disconnect()
        return factors_init
    
    def check_included_columns(self):
        self.db.connect()
        factors_init = "included_columns" in self.db.collection_names()
        self.db.disconnect()
        return factors_init

    def assign_factors(self,factors):
        self.factors = factors
        self.db.connect()
        self.db.store("factors",pd.DataFrame([{"factors":factors}]))
        self.db.disconnect()

    def pull_factors(self):
        self.db.connect()
        self.factors = self.db.retrieve("factors").tail(1)["factors"].item()
        self.db.disconnect()
    
    def assign_included_columns(self,included_columns):
        self.included_columns = included_columns
        self.db.connect()
        self.db.store("included_columns",pd.DataFrame([{"included_columns":included_columns}]))
        self.db.disconnect()

    def pull_included_columns(self):
        self.db.connect()
        self.included_columns = self.db.retrieve("included_columns").tail(1)["included_columns"].item()
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
    
    def pull_recs(self,modeler_type):
        self.db.connect()
        data = self.db.retrieve(f"{modeler_type}_recs")
        self.db.disconnect()
        return data
    
    def pull_models(self,modeler_type):
        self.db.connect()
        data = self.db.retrieve(f"{modeler_type}_models")
        self.db.disconnect()
        return data
    
    def pull_unmodeled(self):
        self.db.connect()
        data = self.db.retrieve("unmodeled")
        self.db.disconnect()
        return data
    