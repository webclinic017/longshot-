from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import math
import numpy as np

## backtesting class to hold different backtesting methods
class Backtester(object):

    # risk oriented backtest with additional floor, hedge and ceiling options
    @classmethod
    def speculation_backtest(self,sim,parameter,start_date,end_date,db):
        signal = parameter["signal"]
        req = parameter["req"]
        classification = parameter["classification"]
        ceiling = parameter["ceiling"]
        floor = parameter["floor"]
        hedge = parameter["hedge"]
        floor_value = -0.05
        hedge_value = 0.25
        positions = 10
        new_sim = []

        sim = sim[(sim["date"] >= start_date) & (sim["date"] <= end_date)]

        ## optimizing
        sim["date_boolean"] = [x.weekday() == 0 for x in sim["date"]]
        sim = sim[sim["date_boolean"]==True]
        sim["risk_boolean"] = sim["beta"] <= sim["beta"].mean()
        sim["return_boolean"] = sim["delta"] > sim["rrr"]
        # sim["quarterly_return_boolean"] = sim["quarterly_delta"] > 0
        sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
        # sim["gain_value_boolean"] = [True in [row[1][f"return_{i}"] >= req for i in range(2,5)] for row in sim.iterrows()]
        # sim["gain_index"] = [min([i if row[1][f"return_{i}"] >= req else 5 for i in range(2,5)]) for row in sim.iterrows()]
        sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
        # sim["floor_boolean"] = sim["gain_index"] > sim["floor_index"]
        sim["floor_boolean"] = 4 >= sim["floor_index"]

        # sim["returns"] = [1 + req if row[1]["gain_value_boolean"] else 1 + row[1][f"return_{4}"] for row in  sim.iterrows()]
        sim["returns"] = [1 + row[1][f"return_{4}"] * row[1]["delta_sign"] for row in  sim.iterrows()]
        sim["floored_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["returns"] for row in sim.iterrows()]

        # sim["hedged_returns"] = [row[1]["returns"] + (req * hedge_value) * -1 if row[1]["returns"] >= 1 \
        #                     else row[1]["returns"] + (req * hedge_value) * 1 for row in sim.iterrows()]

        # sim["floored_hedged_returns"] = [row[1]["floored_returns"] + (req * hedge_value) * -1 if row[1]["floored_returns"] >= 1 \
        #                     else row[1]["floored_returns"] + (req * hedge_value) * 1 for row in sim.iterrows()]

        new_sim.append(sim)

        ns = pd.concat(new_sim)
        # test = ns[(ns["quarterly_return_boolean"]==True) & (ns["return_boolean"]==True) & (ns["risk_boolean"]==True)]
        test = ns[(ns["return_boolean"]==True) & (ns["risk_boolean"]==True)]
        ## filters
        if parameter["value"] != True:
            test["delta"] = test["delta"] * -1
            # test["quarterly_delta"] = test["quarterly_delta"] * -1
            test["classification_prediction"] = [not x for x in test["classification_prediction"]]

        if classification:
            test = test[test["classification_prediction"]==True]
        if ceiling:
            test = test[test["delta"]<=1]
            # test = test[test["quarterly_delta"]<=1]
        
        ## signal requirements
        # test = test[test["delta"]>=signal]
        ledgers = []
        ## ledger creation
        # stuff = ["year","week","ticker","gain_index","floor_index","delta","returns","floored_returns","hedged_returns","floored_hedged_returns"]
        stuff = ["year","week","ticker","delta_sign","delta","returns","floored_returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year","week","delta"])[stuff].groupby(["year","week"],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()
        # if floor:
        #     return_column = "floored_hedged_returns" if hedge else "floored_returns"
        # else:
        #     return_column = "hedged_returns" if hedge else "returns"
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

    # risk oriented backtest with additional floor, hedge and ceiling options
    @classmethod
    def btc_backtest(self,sim,parameter,start_date,end_date,db):
        classification = parameter["classification"]
        ceiling = parameter["ceiling"]
        floor = parameter["floor"]
        floor_value = -0.05
        positions = 1
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

    # risk oriented backtest with additional floor, hedge and ceiling options
    @classmethod
    def financial_backtest(self,sim,parameter,start_date,end_date,db):
        signal = parameter["signal"]
        req = parameter["req"]
        classification = parameter["classification"]
        ceiling = parameter["ceiling"]
        floor = parameter["floor"]
        hedge = parameter["hedge"]
        floor_value = -0.05
        hedge_value = 0.25
        positions = 10
        new_sim = []

        sim = sim[(sim["date"] >= start_date) & (sim["date"] <= end_date)]

        ## optimizing
        sim["date_boolean"] = [x.weekday() == 0 for x in sim["date"]]
        sim = sim[sim["date_boolean"]==True]
        sim["risk_boolean"] = sim["beta"] <= sim["beta"].mean()
        sim["return_boolean"] = sim["delta"] > sim["rrr"]
        sim["quarterly_return_boolean"] = sim["quarterly_delta"] > sim["quarterly_rrr"]
        sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
        # sim["gain_value_boolean"] = [True in [row[1][f"return_{i}"] >= req for i in range(2,5)] for row in sim.iterrows()]
        # sim["gain_index"] = [min([i if row[1][f"return_{i}"] >= req else 5 for i in range(2,5)]) for row in sim.iterrows()]
        sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
        # sim["floor_boolean"] = sim["gain_index"] > sim["floor_index"]
        sim["floor_boolean"] = 4 >= sim["floor_index"]

        # sim["returns"] = [1 + req if row[1]["gain_value_boolean"] else 1 + row[1][f"return_{4}"] for row in  sim.iterrows()]
        sim["returns"] = [1 + row[1][f"return_{4}"] * row[1]["delta_sign"] for row in  sim.iterrows()]
        sim["floored_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["returns"] for row in sim.iterrows()]

        # sim["hedged_returns"] = [row[1]["returns"] + (req * hedge_value) * -1 if row[1]["returns"] >= 1 \
        #                     else row[1]["returns"] + (req * hedge_value) * 1 for row in sim.iterrows()]

        # sim["floored_hedged_returns"] = [row[1]["floored_returns"] + (req * hedge_value) * -1 if row[1]["floored_returns"] >= 1 \
        #                     else row[1]["floored_returns"] + (req * hedge_value) * 1 for row in sim.iterrows()]

        new_sim.append(sim)

        ns = pd.concat(new_sim)
        test = ns[(ns["quarterly_return_boolean"]==True) & (ns["return_boolean"]==True) & (ns["risk_boolean"]==True)]

        ## filters
        if parameter["value"] != True:
            test["delta"] = test["delta"] * -1
            test["quarterly_delta"] = test["quarterly_delta"] * -1
            test["classification_prediction"] = [not x for x in test["classification_prediction"]]

        if classification:
            test = test[test["classification_prediction"]==True]
        if ceiling:
            test = test[test["delta"]<=1]
            test = test[test["quarterly_delta"]<=1]
        
        ## signal requirements
        # test = test[test["delta"]>=signal]
        ledgers = []
        ## ledger creation
        # stuff = ["year","week","ticker","gain_index","floor_index","delta","returns","floored_returns","hedged_returns","floored_hedged_returns"]
        stuff = ["year","week","ticker","delta_sign","quarterly_delta","delta","returns","floored_returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year","week","delta"])[stuff].groupby(["year","week"],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()
        # if floor:
        #     return_column = "floored_hedged_returns" if hedge else "floored_returns"
        # else:
        #     return_column = "hedged_returns" if hedge else "returns"
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
    