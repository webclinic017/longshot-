from pricer.apricer import APricer

class NonAIPricer(APricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = True