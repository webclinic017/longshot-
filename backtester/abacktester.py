import pandas as pd
from parameters.parameters import Parameters as params
class ABacktester(object):

    def __init__(self,portfolio_class,current,start_date,end_date):
        self.current = current
        self.start_date = start_date
        self.end_date = end_date
        self.portfolio_class = portfolio_class
        self.table_name = "trades" if self.current else "historical_trades"
    
    def backtest(self,sim):
        self.portfolio_class.db.connect()
        self.portfolio_class.db.drop(self.table_name)
        backtest_data = sim.copy().dropna()
        parameters = params.parameters(sim.columns)
        for parameter in parameters:
            trades = self.backtest_helper(backtest_data.copy(),parameter,self.start_date,self.end_date)
            self.portfolio_class.db.store(self.table_name,trades)
        self.portfolio_class.db.disconnect()
    
    def backtest_helper(self,sim,parameter,start_date,end_date):
        value = parameter["value"]
        ceiling = parameter["ceiling"]
        # floor = parameter["floor"]
        # floor_value = -0.05
        naming = self.portfolio_class.pricer_class.time_horizon_class.naming_convention
        positions = self.portfolio_class.pricer_class.positions
        sim = sim[(sim["date"] >= start_date) & (sim["date"] <= end_date)]
        ## optimizing
        # sim["date_boolean"] = [x.weekday() == 0 for x in sim["date"]]
        # sim = sim[sim["date_boolean"]==True]

        ##{naming}ly
        sim["risk_boolean"] = sim[f"{naming}ly_beta"] <= sim[f"{naming}ly_beta"].mean()
        sim["return_boolean"] = sim[f"projected_{naming}ly_return"] > sim[f"{naming}ly_rrr"]
        sim["returns"] = [1 + row[1][f"{naming}ly_return"] for row in sim.iterrows()]

        ##weekly
        # sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
        # sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
        # sim["floor_boolean"] = 4 >= sim["floor_index"]
        # sim["returns"] = [1 + row[1][f"return_{4}"] * row[1]["delta_sign"] for row in  sim.iterrows()]
        # sim["floored_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["returns"] for row in sim.iterrows()]
        
        test = sim[(sim["return_boolean"]==True) & (sim["risk_boolean"]==True)]
        # filters
        if value != True:
            test[f"{naming}ly_delta"] = test[f"{naming}ly_delta"] * -1
            if "classification_prediction" in test.columns:
                test["classification_prediction"] = [not x for x in test["classification_prediction"]]

        if "classification" in parameter.keys():
            classification = parameter["classification"]
            if classification:
                test = test[test["classification_prediction"]==True]
                
        if "rank" in parameter.keys():
            rank = parameter["rank"]
            if rank:
                test = test.sort_values("rank",ascending=False)

        if ceiling:
            test = test[test[f"{naming}ly_delta"]<=1]

        ledgers = []
        ## ledger creation
        stuff = ["year",naming,"ticker",f"{naming}ly_delta","returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year",naming,f"{naming}ly_delta"])[stuff].groupby(["year",naming],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()

        # if floor:
        #     return_column = "floored_returns"
        # else:
        return_column = "returns"
        final["actual_returns"] = final[return_column]

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        return final