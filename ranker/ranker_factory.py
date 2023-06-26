from ranker.ranker import Ranker as ranker_list
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses
from ranker.earnings import Earnings
from ranker.rolling import Rolling
from ranker.fast_slow import FastSlow
class RankerFactory(object):

    @classmethod
    def build(self,ranker):
        match ranker:
            case ranker_list.NONE:
                result = None
            case ranker_list.QUARTERLY_STOCK_EARNINGS_RANKER:
                result = Earnings()
            case ranker_list.WEEKLY_STOCK_ROLLING_RANKER:
                result = Rolling(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case ranker_list.WEEKLY_STOCK_FASTSLOW_RANKER:
                result = FastSlow(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case ranker_list.WEEKLY_CRYPTO_FASTSLOW_RANKER:
                result = FastSlow(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case _:
                result = None
        return result