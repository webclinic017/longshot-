from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import math
import numpy as np

## backtesting class to hold different backtesting methods
class Backtester(object):

    # risk oriented backtest utilizes weeklies with additional floor, and ceiling options includes shorts
    @classmethod
    def weeklies_backtest(self,sim,positions,parameter,start_date,end_date,db):
        classification = parameter["classification"]
        ceiling = parameter["ceiling"]
        floor = parameter["floor"]
        floor_value = -0.05
        new_sim = []

        sim = sim[(sim["date"] >= start_date) & (sim["date"] <= end_date)]

        ## optimizing
        sim["date_boolean"] = [x.weekday() == 0 for x in sim["date"]]
        sim = sim[sim["date_boolean"]==True]
        sim["risk_boolean"] = sim["beta"] <= sim["beta"].mean()
        sim["return_boolean"] = sim["delta"] > sim["rrr"]
        sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
        sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
        sim["floor_boolean"] = 4 >= sim["floor_index"]
        sim["returns"] = [1 + row[1][f"return_{4}"] * row[1]["delta_sign"] for row in  sim.iterrows()]
        sim["floored_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["returns"] for row in sim.iterrows()]

        new_sim.append(sim)
        ns = pd.concat(new_sim)
        test = ns[(ns["return_boolean"]==True) & (ns["risk_boolean"]==True)]

        ## filters
        if parameter["value"] != True:
            test["delta"] = test["delta"] * -1
            test["classification_prediction"] = [not x for x in test["classification_prediction"]]

        if classification:
            test = test[test["classification_prediction"]==True]
        if ceiling:
            test = test[test["delta"]<=1]

        ledgers = []
        ## ledger creation
        stuff = ["year","week","ticker","delta_sign","delta","returns","floored_returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year","week","delta"])[stuff].groupby(["year","week"],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()

        if floor:
            return_column = "floored_returns"
        else:
            return_column = "returns"
        final["actual_returns"] = final[return_column]

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        db.store("trades",final)
    

    # risk oriented backtest utilizes quarterlies and weeklies with additional floor, hedge and ceiling options
    @classmethod
    def quarterly_backtest(self,sim,positions,parameter,start_date,end_date,db):
        ceiling = parameter["ceiling"]
        new_sim = []

        sim = sim[(sim["year"] >= start_date.year) & (sim["year"] <= end_date.year)]

        ## optimizing
        sim["risk_boolean"] = sim["quarterly_beta"] <= sim["quarterly_beta"].mean()
        sim["quarterly_return_boolean"] = sim["projected_quarterly_return"] > sim["quarterly_rrr"]
        sim["returns"] = [1 + row[1][f"quarterly_return"] for row in sim.iterrows()]
        new_sim.append(sim)
        ns = pd.concat(new_sim)
        test = ns[(ns["quarterly_return_boolean"]==True) & (ns["risk_boolean"]==True)]

        ## filters
        if parameter["value"] != True:
            test["quarterly_delta"] = test["quarterly_delta"] * -1

        if ceiling:
            test = test[test["quarterly_return"]<=1]

        ledgers = []
        ## ledger creation
        stuff = ["year","quarter","ticker","projected_quarterly_return","returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year","quarter","projected_quarterly_return"])[stuff].groupby(["year","quarter"],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()
        
        return_column = "returns"
        final["actual_returns"] = final[return_column]

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        db.store("trades",final)
    