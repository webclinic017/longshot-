from pricer.atradingpricer import ATradingPricer

class NonAIPricer(ATradingPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = False