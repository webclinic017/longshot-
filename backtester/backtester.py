from tqdm import tqdm
import pandas as pd
class Backtester(object):
    def __init__(self,start_date,end_date,portfolio,initial):
        self.start_date = start_date
        self.end_date = end_date
        self.portfolio = portfolio
        self.initial = initial
        self.values = [True]
        self.ocs = [False]
        self.reqs = [0.05,0.15,0.3]
        self.signals = [0.05,0.15,0.3]

    def reqs_init(self,reqs):
        self.reqs = reqs

    def signals_init(self,signals):
        self.signals = signals

    def values_init(self,values):
        self.values = values
    
    def ocs_init(self,ocs):
        self.ocs = ocs

    def parameters_init(self):
        parameters = []
        for signal in self.signals:
            for req in self.reqs:
                for value in self.values:
                    for oc in self.ocs:
                        parameters.append({"signal":signal,"req":req,"value":value,"oc":oc}) 
        self.parameters = parameters

    def backtest(self,sim,prices):
        for parameter in tqdm(self.parameters):
            self.portfolio.db_subscribe()
            self.portfolio.initialize_backtest_portfolio(parameter,self.initial,self.start_date)
            iterration_sim = sim.copy()
            if parameter["value"] == False:
                iterration_sim["delta"] = iterration_sim["delta"] * -1
            date = self.start_date
            for date in pd.bdate_range(self.start_date,self.end_date):
                self.portfolio.daily_iterration(iterration_sim,date,prices,parameter)
            self.portfolio.db_unsubscribe()

    def backtest_accelerate(self,sim):
        for parameter in tqdm(self.parameters):
            self.portfolio.db_subscribe()
            iterration_sim = sim.copy()
            if parameter["value"] == False:
                iterration_sim["delta"] = iterration_sim["delta"] * -1
            self.portfolio.iterration_accelerate(iterration_sim,self.start_date,self.end_date,parameter)
            self.portfolio.db_unsubscribe()