from tradealgorithm.tradealgorithm import TradeAlgorithm
from tradealgorithm.atradealgorithm import ATradeAlgorithm
from risk.beta_risk import BetaRisk
from returns.required_returns import RequiredReturn

class TradeAlgorithmFactory(object):

    @classmethod
    def build(self,tradealgorithm):
        match tradealgorithm:
            case TradeAlgorithm.RRR_BETA:
                result =  ATradeAlgorithm(returns=RequiredReturn(),risk=BetaRisk())
            case _:
                result = None
        return result