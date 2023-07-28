import pandas as pd
from pricer.pricer import Pricer as pricer_list
from pricer.financial import Financial
from pricer.speculation import Speculation
from pricer.window import Window
from pricer.rolling import Rolling
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class PricerFactory(object):

    @classmethod
    def build(self,pricer):
        match pricer:
            case pricer_list.QUARTERLY_STOCK_FINANCIAL:
                result =  Financial()
            case pricer_list.WEEKLY_STOCK_SPECULATION:
                result =  Speculation(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case pricer_list.WEEKLY_STOCK_ROLLING:
                result = Rolling(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case pricer_list.WEEKLY_STOCK_WINDOW:
                result = Window(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case pricer_list.WEEKLY_CRYPTO_SPECULATION:
                result =  Speculation(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case pricer_list.WEEKLY_CRYPTO_ROLLING:
                result = Rolling(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case pricer_list.WEEKLY_CRYPTO_WINDOW:
                result = Window(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case pricer_list.DAILY_STOCK_SPECULATION:
                result =  Speculation(AssetClasses.STOCKS,TimeHorizons.DAILY)
            case pricer_list.DAILY_STOCK_ROLLING:
                result = Rolling(AssetClasses.STOCKS,TimeHorizons.DAILY)
            case pricer_list.DAILY_STOCK_WINDOW:
                result = Window(AssetClasses.STOCKS,TimeHorizons.DAILY)
            case pricer_list.DAILY_CRYPTO_SPECULATION:
                result =  Speculation(AssetClasses.CRYPTO,TimeHorizons.DAILY)
            case pricer_list.DAILY_CRYPTO_ROLLING:
                result = Rolling(AssetClasses.CRYPTO,TimeHorizons.DAILY)
            case pricer_list.DAILY_CRYPTO_WINDOW:
                result = Window(AssetClasses.CRYPTO,TimeHorizons.DAILY)
            case _:
                result = None
        return result