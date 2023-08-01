import pickle
import pandas as pd
from datetime import datetime,timedelta
class Simulator(object):
    def __init__(self,portfolio):
        self.portfolio = portfolio

    def simulate(self,prices):
        initial = 100000
        parameter = self.portfolio.pull_parameters()
        if parameter.index.size > 0:
            parameter = parameter.iloc[0]
            strat_class = self.portfolio.strat_class
            recs = self.portfolio.strat_class.pull_recs(self.portfolio.modeler_class.name)
            max_date = self.portfolio.pull_portfolio_max_date()
            self.portfolio.initialize_portfolio(parameter,initial,max_date)
            for today in pd.bdate_range(max_date,datetime.now()):
                sim = strat_class.create_rec(recs,today,prices.copy())
                if parameter["value"] == False:
                    sim["delta"] = sim["delta"] * -1
                self.portfolio.daily_iterration(sim,today,prices,parameter)