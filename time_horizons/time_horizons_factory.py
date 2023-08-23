from time_horizons.time_horizons import TimeHorizons
from time_horizons.atime_horizon import ATimeHorizon

class TimeHorizonFactory(object):

    @classmethod
    def build(self,time_horizon):
        match time_horizon:
            case TimeHorizons.DAILY:
                result = ATimeHorizon(name="date"
                                      ,naming_convention="date"
                                      ,y_column="adjopen"
                                      ,y_pivot_number=1
                                      ,model_offset=4
                                      ,rolling=5
                                      ,window=5
                                      ,instances_per_year=365
                                      ,holding_period=1
                                      )
            case TimeHorizons.WEEKLY:
                result = ATimeHorizon(name="week"
                                      ,naming_convention="week"
                                      ,y_column="adjopen"
                                      ,y_pivot_number=1
                                      ,model_offset=4
                                      ,rolling=25
                                      ,window=25
                                      ,instances_per_year=52
                                      ,holding_period=5
                                      )
            # case TimeHorizons.MONTHLY:
            #     result = ATimeHorizon(name="month"
            #                           ,naming_convention="month"
            #                           ,y_column="adjopen"
            #                           ,y_pivot_number=1
            #                           ,model_offset=4
            #                           ,rolling=100
            #                           ,window=100
            #                           ,instances_per_year=12
            #                           ,holding_period=20
            #                           )
            case TimeHorizons.QUARTERLY:
                result = ATimeHorizon(name="quarter"
                                      ,naming_convention="quarter"
                                      ,y_column="adjopen"
                                      ,y_pivot_number=1
                                      ,model_offset=4
                                      ,rolling=260
                                      ,window=260
                                      ,instances_per_year=4
                                      ,holding_period=60
                                      )
            # case TimeHorizons.YEARLY:
            #     result = ATimeHorizon(name="year"
            #                           ,naming_convention="year"
            #                           ,y_column="adjopen"
            #                           ,y_pivot_number=1
            #                           ,model_offset=4
            #                           ,rolling=1300
            #                           ,window=1300
            #                           ,instances_per_year=1
            #                           ,holding_period=260
            #                           )
            case _:
                result = None
        return result