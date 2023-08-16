import math
import numpy as np
from returns.areturns import AReturns

class RequiredReturn(AReturns):
    def __init__(self):
        super.__init__("RR")
    
    def required_returns(self,market_return,atime_horizon,simulation,current,yields):
        naming = atime_horizon.naming_convention
        n = atime_horizon.instances_per_year
        simulation = simulation.merge(yields.drop(["quarter","month"],axis=1),on=["year","week"],how="left").dropna()
        week_col = "adjclose" if current else "prev_close"
        start_col = week_col
        simulation[f"market_{naming}ly_return"] = math.exp(np.log(market_return)/n)
        simulation[f"projected_{naming}ly_return"] = (simulation[f"price_prediction"] - simulation[start_col]) / simulation[start_col]
        simulation[f"{naming}ly_delta"] = [abs(x) for x in simulation[f"projected_{naming}ly_return"]]
        simulation[f"{naming}ly_delta_sign"] = [1 if x >= 0 else -1 for x in simulation[f"projected_{naming}ly_return"]]
        for maturity in [1,2,10]:
            simulation[f"{naming}ly_rrr_tyield{maturity}"] = simulation[f"{naming}ly_yield{maturity}"] + simulation[f"{naming}ly_beta"] * (simulation[f"market_{naming}ly_return"] - simulation[f"{naming}ly_yield{maturity}"]) - 1
        simulation = simulation.groupby(["year","quarter","month","week","date","ticker"]).mean().reset_index()
        return simulation