

class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True]
        ceilings = [True]
        classifications = [False]
        market_returns = [1.15]
        shorts = [False]
        ranks = [False]
        parameters = []
        buy_days = [0,1,2,3,4]
        sell_days = [1,2,3,4,5]
        for value in values:
            for ceiling in ceilings:
                for classification in classifications:
                    for rank in ranks:
                        for short in shorts:
                            for market_return in market_returns:
                                for buy_day in buy_days:
                                    for sell_day in sell_days:
                                        parameter = {"value":value
                                                ,"ceiling":ceiling
                                                ,"classification":classification
                                                ,"rank":rank
                                                ,"short":short
                                                ,"market_return":market_return
                                                ,"buy_day":buy_day
                                                ,"sell_day":sell_day
                                            }
                                        parameters.append(parameter)
        return parameters