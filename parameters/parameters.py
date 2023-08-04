

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
        parameters = []
        floor_values= [1]
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