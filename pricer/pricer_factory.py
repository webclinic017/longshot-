from pricer.pricer import Pricer as pricer_list
from pricer.window import Window
from pricer.rolling import Rolling
# from pricer.mean_reversion import MeanReversion
# from pricer.dev_rolling import DevRolling
# from pricer.dailybreakout import DailyBreakout

from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class PricerFactory(object):

    @classmethod
    def build(self,pricer):
        match pricer:
            case pricer_list.DAILY_STOCK_ROLLING:
                result =  Rolling(AssetClasses.STOCKS,TimeHorizons.DAILY)
            case pricer_list.WEEKLY_STOCK_ROLLING:
                result =  Rolling(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case pricer_list.QUARTERLY_STOCK_ROLLING:
                result =  Rolling(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
            case pricer_list.MONTHLY_STOCK_ROLLING:
                result =  Rolling(AssetClasses.STOCKS,TimeHorizons.MONTHLY)
            case pricer_list.YEARLY_STOCK_ROLLING:
                result =  Rolling(AssetClasses.STOCKS,TimeHorizons.YEARLY)

            case pricer_list.DAILY_STOCK_WINDOW:
                result =  Window(AssetClasses.STOCKS,TimeHorizons.DAILY)
            case pricer_list.WEEKLY_STOCK_WINDOW:
                result =  Window(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case pricer_list.QUARTERLY_STOCK_WINDOW:
                result =  Window(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
            case pricer_list.MONTHLY_STOCK_WINDOW:
                result =  Window(AssetClasses.STOCKS,TimeHorizons.MONTHLY)
            case pricer_list.YEARLY_STOCK_WINDOW:
                result =  Window(AssetClasses.STOCKS,TimeHorizons.YEARLY)
            
            case pricer_list.WEEKLY_CRYPTO_ROLLING:
                result = Rolling(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case pricer_list.WEEKLY_CRYPTO_WINDOW:
                result = Window(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case pricer_list.DAILY_CRYPTO_ROLLING:
                result = Rolling(AssetClasses.CRYPTO,TimeHorizons.DAILY)
            case pricer_list.DAILY_CRYPTO_WINDOW:
                result = Window(AssetClasses.CRYPTO,TimeHorizons.DAILY)

            # case pricer_list.DAILY_STOCK_MEANREVERSION:
            #     result = MeanReversion(AssetClasses.STOCKS,TimeHorizons.DAILY)
            # case pricer_list.DAILY_STOCK_DEVROLLING:
            #     result = DevRolling(AssetClasses.STOCKS,TimeHorizons.DAILY)
            # case pricer_list.DAILY_STOCK_BREAKOUT:
            #     result = DailyBreakout(AssetClasses.STOCKS,TimeHorizons.DAILY)
            case _:
                result = None
        return result