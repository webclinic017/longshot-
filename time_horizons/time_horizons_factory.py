from time_horizons.time_horizons import TimeHorizons
from time_horizons.atime_horizon import ATimeHorizon

class TimeHorizonFactory(object):

    @classmethod
    def build(self,time_horizon):
        match time_horizon:
            case TimeHorizons.WEEKLY:
                result = ATimeHorizon(naming_convention="week"
                                       ,model_date_offset=0
                                       ,y_pivot_column="adjclose"
                                       ,y_pivot_number=1
                                       ,y_price_returns_offset=5
                                       ,prediction_pivot_column="week"
                                       ,prediction_pivot_number=1
                                       ,rolling_number=14
                                       ,n=52)
            case TimeHorizons.DAILY:
                result =  ATimeHorizon(naming_convention="date"
                                       ,model_date_offset=0
                                       ,y_pivot_column="adjclose"
                                       ,y_pivot_number=1
                                       ,y_price_returns_offset=1
                                       ,prediction_pivot_column="date"
                                       ,prediction_pivot_number=1
                                       ,rolling_number=20
                                       ,n=52)
            case _:
                result = None
        return result