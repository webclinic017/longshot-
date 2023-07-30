import math
import numpy as np

class RequiredReturn(object):
    def __init__(self):
        self.name = "RR"

    def returns(self,market_return,atime_horizon,simulation,current,yields):
        naming = atime_horizon.naming_convention
        n = atime_horizon.instances_per_year
        simulation = simulation.merge(yields.drop(["year","quarter","month","week"],axis=1),on="date",how="left").dropna()
        week_col = "adjclose" if current else "prev_close"
        start_col = "quarter_start" if naming == "quarter" else week_col
        simulation[f"market_{naming}ly_return"] = math.exp(np.log(market_return)/n)
        simulation[f"projected_{naming}ly_return"] = (simulation[f"price_prediction"] - simulation[start_col]) / simulation[start_col]
        simulation[f"{naming}ly_delta"] = [abs(x) for x in simulation[f"projected_{naming}ly_return"]]
        simulation[f"{naming}ly_delta_sign"] = [1 if x >= 0 else -1 for x in simulation[f"projected_{naming}ly_return"]]
        for maturity in [1,2,10]:
            simulation[f"{naming}ly_rrr_tyield{maturity}"] = simulation[f"{naming}ly_yield{maturity}"] + simulation[f"{naming}ly_beta"] * (simulation[f"market_{naming}ly_return"] - simulation[f"{naming}ly_yield{maturity}"]) - 1
        simulation = simulation.groupby(["year","quarter","month","week","date","ticker"]).mean().reset_index()
        return simulation