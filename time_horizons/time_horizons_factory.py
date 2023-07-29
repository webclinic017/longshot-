from time_horizons.time_horizons import TimeHorizons
from time_horizons.atime_horizon import ATimeHorizon

class TimeHorizonFactory(object):

    @classmethod
    def build(self,time_horizon):
        match time_horizon:
            case TimeHorizons.WEEKLY:
                result = ATimeHorizon(name="week"
                                      ,naming_convention="week"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,rolling=14
                                      ,window=10
                                      )
            case TimeHorizons.DAILY:
                result = ATimeHorizon(name="date"
                                      ,naming_convention="date"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,rolling=14
                                      ,window=10
                                      )
            case _:
                result = None
        return result