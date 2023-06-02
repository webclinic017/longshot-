import pandas as pd
from pricer.pricer import Pricer as pricer_list
from pricer.financial import Financial
from time_horizons.time_horizons import TimeHorizons as timehorizons

class PricerFactory(object):

    @classmethod
    def build(self,pricer):
        match pricer:
            case pricer_list.QUARTERLY_STOCK_FINANCIAL:
                result =  Financial()
            case _:
                result = None
        return result