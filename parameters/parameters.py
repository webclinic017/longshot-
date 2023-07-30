

class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True,False]
        ceilings = [True,False]
        classifications = [False]
        ranks = [False]
        risks = ["rrr","flat","none"]
        tyields = ["tyield1","tyield2","tyield10"]
        parameters = []
        floor_values= [0.5,1]
        for value in values:
            for ceiling in ceilings:
                for classification in classifications:
                    for rank in ranks:
                        for risk in risks:
                            for floor_value in floor_values:
                                for tyield in tyields:
                                    parameter = {"value":value
                                            ,"ceiling":ceiling
                                            ,"classification":classification
                                            ,"rank":rank
                                            ,"risk":risk
                                            ,"floor_value":floor_value
                                            ,"tyields":tyield
                                        }
                                    parameters.append(parameter)
        return parameters