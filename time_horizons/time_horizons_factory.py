from time_horizons.time_horizons import TimeHorizons
from time_horizons.atime_horizon import ATimeHorizon

class TimeHorizonFactory(object):

    @classmethod
    def build(self,time_horizon):
        match time_horizon:
            case TimeHorizons.QUARTERLY:
                result =  ATimeHorizon("quarter",365,"adjclose",4,"year",1,4,4)
            case TimeHorizons.WEEKLY:
                result =  ATimeHorizon("week",0,"adjclose",1,"week",1,14,52)
            case TimeHorizons.YEARLY:
                result =  ATimeHorizon("year",365,"adjclose",1,"year",1,1,1)
            case _:
                result = None
        return result