from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import math
import numpy as np
pd.options.mode.chained_assignment = None

## backtesting class to hold different backtesting methods
class QuarterlyBacktester(object):

    # risk oriented backtest utilizes quarterlies and weeklies with additional floor, hedge and ceiling options
    @classmethod
    def backtest(self,sim,positions,parameter,start_date,end_date,db):
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
    