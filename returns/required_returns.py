import math
import numpy as np
from returns.areturns import AReturns

class RequiredReturn(AReturns):
    def __init__(self):
        super().__init__("RR")
    
    def required_returns(self,market_return,atime_horizon,simulation,current,yields):
        # n = atime_horizon.instances_per_year
        # simulation = simulation.merge(yields.drop(["year","quarter","month","week"],axis=1),on="date",how="left").dropna()
        week_col = "adjopen" if current else "prev_open"
        start_col = week_col
        # simulation["market_return"] = math.exp(np.log(market_return)/n)
        simulation["projected_return"] = (simulation[f"price"] - simulation[start_col]) / simulation[start_col]
        simulation["delta"] = [abs(x) for x in simulation[f"projected_return"]]
        simulation["delta_sign"] = [1 if x >= 0 else -1 for x in simulation[f"projected_return"]]
        # for maturity in [1,2,10]:
        #     simulation[f"rrr_tyield{maturity}"] = simulation[f"yield{maturity}"] + simulation[f"beta"] * (simulation[f"market_return"] - simulation[f"yield{maturity}"]) - 1
        return simulation.groupby(["date","GICS Sector","ticker"]).mean().reset_index()