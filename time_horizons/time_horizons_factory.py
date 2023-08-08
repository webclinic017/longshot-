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
                                      ,model_offset=4
                                      ,rolling=50
                                      ,window=10
                                      ,instances_per_year=52
                                      ,holding_period=5
                                      )
            case TimeHorizons.DAILY:
                result = ATimeHorizon(name="date"
                                      ,naming_convention="date"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,model_offset=4
                                      ,rolling=20
                                      ,window=5
                                      ,instances_per_year=365
                                      ,holding_period=1
                                      )
            case TimeHorizons.MONTHLY:
                result = ATimeHorizon(name="month"
                                      ,naming_convention="month"
                                      ,y_column="adjclose"
                                      ,y_pivot_number=1
                                      ,model_offset=4
                                      ,rolling=12
                                      ,window=3
                                      ,instances_per_year=12
                                      ,holding_period=20
                                      )
            case _:
                result = None
        return result