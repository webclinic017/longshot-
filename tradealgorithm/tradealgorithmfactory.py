from tradealgorithm.tradealgorithm import TradeAlgorithm
from atradealgorithm import ATradeAlgorithm
from modeler_strats.universal_modeler import UniversalModeler
from risk.beta_risk import BetaRisk
from returns.required_returns import RequiredReturn
from database.adatabase import ADatabase
from backtester.abacktester import ABacktester
from returns.products import Products
class TradeAlgorithmFactory(object):

    @classmethod
    def build(self,pricer):
        match pricer:
            case TradeAlgorithm.RRR_BETA:
                result =  ATradeAlgorithm(returns=RequiredReturn(),risk=BetaRisk(),modeler=UniversalModeler())
            case _:
                result = None
        return result