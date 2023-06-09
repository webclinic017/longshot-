from ranker.aranker import ARanker

class NonAIRanker(ARanker):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = False