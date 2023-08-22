
## description: parameter class to store and create parameters for backtests
class Parameters(object):

    @classmethod
    def parameters(self):
        values = [True,False]
        classifications = [True,False]
        ranks = [True,False]
        buy_days = [1]
        floors = [0]
        ceilings = [1]
        constituents = [100]
        parameters = []
        for constituent in constituents:
            for value in values:
                for ceiling in ceilings:
                    for classification in classifications:
                        for rank in ranks:
                            for floor in floors:
                                for ceiling in ceilings:
                                    for buy_day in buy_days:
                                        parameter = {
                                                "constituent":constituent
                                                ,"value":value
                                                ,"ceiling":ceiling
                                                ,"classification":classification
                                                ,"rank":rank
                                                ,"floor":floor
                                                ,"ceiling":ceiling
                                                ,"buy_day":buy_day
                                            }
                                        parameters.append(parameter)
        return parameters