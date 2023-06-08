

class Parameters(object):

    @classmethod
    def parameters(self,simulation_columns):
        values = [True,False]
        ceilings = [True,False]
        parameters = []
        for value in values:
            for ceiling in ceilings:
                parameter = {"value":value
                                ,"ceiling":ceiling
                            }
                if "classification_prediction" in simulation_columns:
                    for classification in [True,False]:
                        parameter["classification"] = classification
                        if "rank_prediction" in simulation_columns:
                            for rank in [True,False]:
                                parameter["rank"] = rank
                                parameters.append(parameter)
                        else:
                            parameters.append(parameter)
                else:
                    parameters.append(parameter)
        return parameters