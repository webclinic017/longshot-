from time_horizons.time_horizons_factory import TimeHorizonFactory
from database.market import Market
from database.sec import SEC
class AClassifier(object):

    def __init__(self,asset_class,time_horizon):
        self.asset_class = asset_class
        self.time_horizon = time_horizon
        self.intialize_time_horizon()
        self.market = Market()
        self.sec = SEC()
        self.pull_sp500()
    
    def pull_sp500(self):
        self.market.connect()
        self.sp500 = self.market.retrieve("sp500")
        self.sp500 = self.sp500.rename(columns={"Symbol":"ticker"})
        self.market.disconnect()

    def intialize_time_horizon(self):
        self.time_horizon_class = TimeHorizonFactory.build(self.time_horizon)
        
    def pull_sim(self):
        self.db.connect()
        sim = self.db.retrieve("sim")
        self.db.disconnect()
        return sim