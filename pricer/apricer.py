from time_horizons.time_horizons_factory import TimeHorizonFactory
from database.market import Market
from database.sec import SEC
from database.adatabase import ADatabase

class APricer(object):
    
    def __init__(self,asset_class,time_horizon):
        self.time_horizon_class = TimeHorizonFactory.build(time_horizon)
        self.asset_class = asset_class
        self.market = Market()
        self.sec = SEC()
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
    
    def pull_models(self):
        self.db.connect()
        models = self.db.retrieve("models")
        self.db.disconnect()
        return models
    
    # def sim_processor(self,simulation):
    #     simulation[self.time_horizon_class.prediction_pivot_column] = simulation[self.time_horizon_class.prediction_pivot_column] + self.time_horizon_class.prediction_pivot_number
    #     return simulation