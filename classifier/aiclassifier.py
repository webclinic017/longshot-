from classifier.aclassifier import AClassifier

class AIClassifier(AClassifier):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = True
    

    def sim_processor(self,simulation):
        simulation[self.time_horizon_class.prediction_pivot_column] = simulation[self.time_horizon_class.prediction_pivot_column] + self.time_horizon_class.prediction_pivot_number
        return simulation