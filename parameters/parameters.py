

class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True]
        ceilings = [True]
        classifications = [False]
        market_returns = [1.15]
        shorts = [False]
        ranks = [False]
        risks = ["flat","rrr","none"]
        tyields = ["tyields","tyields2","tyields10"]
        parameters = []
        buy_days = [1]
        sell_days = [5,10,15,20]
        floor_values= [0.5,1]
        for value in values:
            for ceiling in ceilings:
                for classification in classifications:
                    for rank in ranks:
                        for short in shorts:
                            for market_return in market_returns:
                                for buy_day in buy_days:
                                    for sell_day in sell_days:
                                        for risk in risks:
                                            for floor_value in floor_values:
                                                for tyield in tyields:
                                                    parameter = {"value":value
                                                            ,"ceiling":ceiling
                                                            ,"classification":classification
                                                            ,"rank":rank
                                                            ,"short":short
                                                            ,"market_return":market_return
                                                            ,"buy_day":buy_day
                                                            ,"sell_day":sell_day
                                                            ,"risk":risk
                                                            ,"floor_value":floor_value
                                                            ,"tyields":tyield
                                                        }
                                                    parameters.append(parameter)
        return parameters