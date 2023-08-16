from strategy.strategy import Strategy
from strategy.astrategy import AStrategy
from risk.beta_risk import BetaRisk
from returns.required_returns import RequiredReturn

class StrategyFactory(object):

    @classmethod
    def build(self,strategy):
        match strategy:
            case Strategy.RRR_BETA:
                result =  AStrategy(returns=RequiredReturn(),risk=BetaRisk())
            case _:
                result = None
        return result