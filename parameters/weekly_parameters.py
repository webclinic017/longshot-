

class WeeklyParameters(object):

    @classmethod
    def parameters(self):
        values = [True,False]
        classifications = [True,False]
        ceilings = [True,False]
        floors = [True,False]
        parameters = []
        for value in values:
            for classification in classifications:
                for ceiling in ceilings:
                    for floor in floors:
                        parameter = {
                                    "value":value
                                    ,"classification":classification
                                    ,"ceiling":ceiling
                                    ,"floor":floor
                                    }
                        parameters.append(parameter)
        return parameters