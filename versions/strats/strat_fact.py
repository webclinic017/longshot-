import pandas as pd
from strats.btc_brute import BTCBrute
from strats.btc_spec import BTCSpec
from strats.equity_rolling import EquityRolling
from strats.financial import Financial
from strats.speculation import Speculation
from strats.verified_strats import VerifiedStrats
class StratFactory(object):

    @classmethod
    def build_strat(self,strat_name):
        training_year = 4
        match strat_name:
            case "bitcoin_speculation":
                strat = BTCSpec(training_year)
                strat.pull_included_columns()
                strat.pull_factors()
            case "bitcoin_brute":
                strat = BTCBrute()
            case "financial":
                strat = Financial(training_year)
                strat.pull_included_columns()
                strat.pull_factors()
            case "speculation":
                strat = Speculation(training_year)
                strat.pull_included_columns()
                strat.pull_factors()
            case "equity_rolling":
                strat = EquityRolling()
            case _:
                strat = None
        return strat