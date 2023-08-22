
## description: parameter class to store and create parameters for backtests
class Parameters(object):
    
    @classmethod
    def parameters_lite(self,lookbacks):
        parameters = []
        values = [True,False]
        strategies = ["rolling","window"]
        volatilities = [0.1,0.5]
        for strategy in strategies:
            for value in values:
                for lookback in lookbacks:
                    for volatility in volatilities:
                                    parameter = {
                                            "strategy":strategy
                                            ,"value":value
                                            ,"lookback":lookback
                                            ,"volatility":volatility
                                        }
                                    parameters.append(parameter)
        return parameters