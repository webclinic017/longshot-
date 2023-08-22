
## description: parameter class to store and create parameters for backtests
class Parameters(object):
    
    @classmethod
    def parameters_lite(self,lookbacks):
        parameters = []
        values = [True,False]
        strategies = ["rolling","window"]
        for strategy in strategies:
            for value in values:
                for lookback in lookbacks:
                    parameter = {
                            "strategy":strategy
                            ,"value":value
                            ,"lookback":lookback
                        }
                    parameters.append(parameter)
        return parameters