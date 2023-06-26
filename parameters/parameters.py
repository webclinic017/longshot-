

class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True]
        ceilings = [True]
        classifications = [True,False]
        market_returns = [1.15]
        shorts = [False]
        ranks = [True,False]
        risks = [True]
        parameters = []
        buy_days = [1]
        sell_days = [5]
        for value in values:
            for ceiling in ceilings:
                for classification in classifications:
                    for rank in ranks:
                        for short in shorts:
                            for market_return in market_returns:
                                for buy_day in buy_days:
                                    for sell_day in sell_days:
                                        for risk in risks:
                                            parameter = {"value":value
                                                    ,"ceiling":ceiling
                                                    ,"classification":classification
                                                    ,"rank":rank
                                                    ,"short":short
                                                    ,"market_return":market_return
                                                    ,"buy_day":buy_day
                                                    ,"sell_day":sell_day
                                                    ,"risk":risk
                                                }
                                            parameters.append(parameter)
        return parameters