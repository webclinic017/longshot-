
## description: parameter class to store and create parameters for backtests
class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True]
        ceilings = [False]
        classifications = [False]
        ranks = [False]
        risks = ["flat","rrr","none"]
        tyields = ["tyield1"]
        buy_days = [1]
        floor_values= [1]
        parameters = []
        for value in values:
            for ceiling in ceilings:
                for classification in classifications:
                    for rank in ranks:
                        for risk in risks:
                            for floor_value in floor_values:
                                for tyield in tyields:
                                    for buy_day in buy_days:
                                        parameter = {"value":value
                                                ,"ceiling":ceiling
                                                ,"classification":classification
                                                ,"rank":rank
                                                ,"risk":risk
                                                ,"floor_value":floor_value
                                                ,"tyields":tyield
                                                ,"buy_day":buy_day
                                            }
                                        parameters.append(parameter)
        return parameters
    
    @classmethod
    def parameters_lite(self,lookbacks):
        values = [True,False]
        parameters = []
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