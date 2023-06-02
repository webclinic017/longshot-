from ranker.ranker import Ranker as ranker_list
from time_horizons.time_horizons import TimeHorizons as timehorizons

class RankerFactory(object):

    @classmethod
    def build(self,ranker):
        match ranker:
            case ranker_list.NONE:
                result = None
            case _:
                result = None
        return result