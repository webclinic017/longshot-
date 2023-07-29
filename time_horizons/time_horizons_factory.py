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
            case TimeHorizons.MONTHLY:
                result = ATimeHorizon(name="month"
                                      ,naming_convention="month"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,rolling=12
                                      ,window=12
                                      )
            case TimeHorizons.QUARTERLY:
                result = ATimeHorizon(name="date"
                                      ,naming_convention="date"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,rolling=20
                                      ,window=4
                                      )
            case TimeHorizons.YEARLY:
                result = ATimeHorizon(name="date"
                                      ,naming_convention="date"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,rolling=5
                                      ,window=1
                                      )
            case _:
                result = None
        return result