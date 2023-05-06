import pandas as pd
from modeler_strats.universal_modeler import UniversalModeler
from modeler_strats.subindustry_modeler import SubIndustryModeler
from modeler_strats.ticker_modeler import TickerModeler
from modeler_strats.industry_modeler import IndustryModeler

class ModelerStratFactory(object):

    @classmethod
    def build_modeler_strat(self,modeler_type):
        match modeler_type:
            case "universal":
                portfolio = UniversalModeler()
            case "industry":
                portfolio = IndustryModeler()
            case "subindustry":
                portfolio = SubIndustryModeler()
            case "ticker":
                portfolio = TickerModeler()
            case _:
                portfolio = None
        return portfolio