from portfolio.universal_portfolio import UniversalPortfolio
class PortfolioFactory(object):

    @classmethod
    def build_portfolio(self,portfolio_type,strat_class,modeler_class,diversifier_class,positions,state):
        match portfolio_type:
            case "universal":
                portfolio = UniversalPortfolio(strat_class,modeler_class,diversifier_class,positions,state)
            case _:
                portfolio = None
        return portfolio