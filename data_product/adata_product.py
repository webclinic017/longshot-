from time_horizons.time_horizons_factory import TimeHorizonFactory
from database.market import Market
from database.adatabase import ADatabase

# description: class for data products
class ADataProduct(object):
    
    def __init__(self,asset_class,time_horizon):
        self.time_horizon_class = TimeHorizonFactory.build(time_horizon)
        self.asset_class = asset_class
        self.market = Market()
        self.pull_sp500()
    
    def initialize(self):
        self.name = f"{self.time_horizon_class.naming_convention}ly_{self.asset_class.value}_{self.naming_suffix}"
        self.db = ADatabase(self.name)
    
    def pull_sp500(self):
        self.market.connect()
        self.sp500 = self.market.retrieve("sp500")
        self.sp500 = self.sp500.rename(columns={"Symbol":"ticker"})
        self.market.disconnect()
        
    def pull_sim(self):
        self.db.connect()
        sim = self.db.retrieve("sim")
        self.db.disconnect()
        return sim
    
    def pull_predictions(self):
        self.db.connect()
        predictions = self.db.retrieve("predictions")
        self.db.disconnect()
        return predictions
    
    def pull_recs(self):
        self.db.connect()
        predictions = self.db.retrieve("recs")
        self.db.disconnect()
        return predictions
    
    def drop_sim(self):
        self.db.connect()
        self.db.drop("sim")
        self.db.disconnect()
        
    def drop_predictions(self):
        self.db.connect()
        self.db.drop("predictions")
        self.db.disconnect()
    
    def drop_recommendations(self):
        self.db.connect()
        self.db.drop("recs")
        self.db.disconnect()