from classifier.aclassifier import AClassifier

class AStrategyClassifier(AClassifier):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.factors = []
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","rank"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","rank"]
        self.all_columns = self.factors + self.included_columns