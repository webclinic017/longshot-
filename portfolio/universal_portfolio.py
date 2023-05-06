import pandas as pd
from portfolio.portfolio_utils import PortfolioUtils as portutil
import pickle
from datetime import datetime, timedelta
from portfolio.aportfolio import APortfolio
from database.adatabase import ADatabase
class UniversalPortfolio(APortfolio):
    
    def __init__(self,strat_class,modeler_class,diversifier_class,positions,state):
        super().__init__(strat_class,modeler_class,diversifier_class,positions,state)
        self.db = ADatabase(f"universal_{self.strat_class.name}_{modeler_class.name}_{diversifier_class.name}_{self.state.value}")
    