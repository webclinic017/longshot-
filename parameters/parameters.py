

class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True,False]
        ceilings = [True,False]
        classification = [True,False]
        market_returns = [1.15,1.5,2]
        shorts = [True,False]
        rank = [True,False]
        parameters = []
        for value in values:
            for ceiling in ceilings:
                for classification in [True,False]:
                    for rank in [True,False]:
                        for short in shorts:
                            for market_return in market_returns:
                                parameter = {"value":value
                                        ,"ceiling":ceiling
                                        ,"classification":classification
                                        ,"rank":rank
                                        ,"short":short
                                        ,"market_return":market_return
                                    }
                                parameters.append(parameter)
        return parameters