from pricer.atradingpricer import ATradingPricer
import pandas as pd

class NonAIPricer(ATradingPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = False
    
    def create_sim(self):
        self.training_set()
        self.db.connect()
        self.db.store("sim",self.training_data)
        self.db.disconnect()
        return self.training_data