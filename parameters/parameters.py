

class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True,False]
        ceilings = [True,False]
        classification = [True,False]
        rank = [True,False]
        parameters = []
        for value in values:
            for ceiling in ceilings:
                for classification in [True,False]:
                    for rank in [True,False]:
                        parameter = {"value":value
                                ,"ceiling":ceiling
                                ,"classification":classification
                                ,"rank":rank
                            }
                        parameters.append(parameter)
        return parameters