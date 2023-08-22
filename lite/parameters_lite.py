
## description: parameter class to store and create parameters for backtests
class Parameters(object):
    
    @classmethod
    def parameters_lite(self,lookbacks,holding_periods,ceilings,floors,volatilities,local_mins):
        parameters = []
        values = [True]
        industry_weighteds = [True]
        weekends = [True]
        strategies = ["rolling","window"]
        constituents = [100]
        for strategy in strategies:
            for constituent in constituents:
                for value in values:
                    for lookback in lookbacks:
                        for ceiling in ceilings:
                            for floor in floors:
                                for holding_period in holding_periods:
                                    for volatility in volatilities:
                                        for local_min in local_mins:
                                            for industry_weighted in industry_weighteds:
                                                for weekend in weekends:
                                                    parameter = {
                                                            "strategy":strategy
                                                            ,"value":value
                                                            ,"lookback":lookback
                                                            ,"holding_period":holding_period
                                                            ,"floor":floor
                                                            ,"ceiling":ceiling
                                                            ,"volatility":volatility
                                                            ,"local_min":local_min
                                                            ,"industry_weighted":industry_weighted
                                                            ,"weekend":weekend
                                                            ,"constituent":constituent
                                                        }
                                                    parameters.append(parameter)
        return parameters