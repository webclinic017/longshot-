from time_horizons.time_horizons import TimeHorizons
from time_horizons.atime_horizon import ATimeHorizon

class TimeHorizonFactory(object):

    @classmethod
    def build(self,time_horizon):
        match time_horizon:
            case TimeHorizons.QUARTERLY:
                result =  ATimeHorizon("quarter","year",4)
            case TimeHorizons.WEEKLY:
                result =  ATimeHorizon("week","week",1)
            case TimeHorizons.YEARLY:
                result =  ATimeHorizon("year","year",1)
            case _:
                result = None
        return result