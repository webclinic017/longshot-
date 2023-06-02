

class QuarterlyParameters(object):

    @classmethod
    def parameters(self):
        values = [True,False]
        ceilings = [True,False]
        classifications = [True,False]
        parameters = []
        for value in values:
            for classification in classifications:
                for ceiling in ceilings:
                    parameter = {"value":value
                                ,"ceiling":ceiling
                                ,"classification":classification
                                }
                    parameters.append(parameter)
        return parameters