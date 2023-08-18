import warnings
import pandas as pd
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# Description: class to backtest different trading algorithms
        # self.current: dictates whether or not to output recommendations or backtests
        # self.start_date: start date of the backtest
        # self.end_date: end date of the backtest
        # self.strategy: algorithm class
        # self.table_name: name of the database to store the historical trades
class ABacktester(object):

    def __init__(self,strategy,current,start_date,end_date):
        self.current = current
        self.start_date = start_date
        self.end_date = end_date
        self.strategy = strategy
        self.table_name = "trades" if self.current else "historical_trades"
    
    ## main backtest function
    ## sim: simulation data 
    ## parameter: the parameter of the backtest
    ## rec: whether to output the recommendation or the backtest
    def backtest(self,sim,parameter,rec):
        final_data = sim.dropna()
        sell_day = self.strategy.pricer_class.time_horizon_class.holding_period
        mod_val = int(sell_day / 5)
        buy_day = parameter["buy_day"]
        sp100 = self.strategy.pricer_class.sp100
        constituent = parameter["constituent"]
        if constituent == 100:
            final_data = final_data[final_data["ticker"].isin(list(sp100["ticker"].unique()))]
        if not rec:
            naming = self.strategy.pricer_class.time_horizon_class.naming_convention
            if naming != "date":
                final_data["week"] = [x.week for x in final_data["date"]]
                final_data["day"] = [x.weekday() for x in final_data["date"]]
                final_data = final_data[final_data["week"] % mod_val == 0]
                final_data = final_data[final_data["day"]==buy_day]
        trades = self.backtest_helper(final_data,parameter,self.start_date,self.end_date,rec)
        return trades
    
    ## description: return modification for calculations
    ## sim: simulation
    ## naming: name of the time frame for calculation purposes
    def backtest_return_helper(self,sim):
        sim["returns"] = sim["return"] + 1
        return sim
    
    ## description: heavy lifting function filtering the simulation based on parameters
    ## sim: simulation data
    ## parameter: parameters dictating the filtering of trades
    ## start_date: start date of the backtest
    ## end_date: end date of the backtest
    ## current: whether to output recs or backtest trades
    def backtest_helper(self,sim,parameter,start_date,end_date,current):

        value = parameter["value"]
        ceiling = parameter["ceiling"]
        classification = parameter["classification"]
        floor = parameter["floor"]
        rank = parameter["rank"]
        positions = self.strategy.positions
        sim = sim[(sim["date"] >= start_date) & (sim["date"] <= end_date)]

        if not value:
            sim[f"delta_sign"] = sim[f"delta_sign"] * -1
            sim["classification"] = [int(not x) for x in sim["classification"]]

        sim = sim[sim[f"delta_sign"]==1]
        return_column = "returns"
        columns = ["date","ticker",f"delta",f"delta_sign"]

        if not current:
            sim = self.backtest_return_helper(sim)
            sim["actual_returns"] = sim[return_column]
            columns.append("actual_returns")

        sim = sim[sim[f"delta"]>=floor]
        sim = sim[sim[f"delta"]<=ceiling]
        
        if rank:
            sim = sim[sim["rank"]>=self.strategy.ranker_class.lower_bound]
            sim = sim.sort_values(["date","rank"]).groupby(["date","GICS Sector"],sort=False).last().reset_index()    
        
        if classification:
            sim = sim[sim["classification"]==1.0]

        ledgers = []
        if current:
            sim = sim[sim["date"]==sim["date"].max()]
        
        for i in range(positions):    
            ledger = sim.sort_values(["date",f"delta"])[columns].groupby("date",sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()
        final["iteration"] = parameter["iteration"]

        ## storing
        return final