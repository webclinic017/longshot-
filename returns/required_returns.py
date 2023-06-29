import math
import numpy as np
from returns.products import Products as return_prods
from datetime import datetime
class RequiredReturn(object):
    def __init__(self):
        self.name = "RR"

    def returns(self,market_return,atime_horizon,simulation,current,yields):
        naming = atime_horizon.naming_convention
        n = atime_horizon.n
        simulation = simulation.merge(yields[["year",naming,f"{naming}ly_yield"]],on=["year",naming],how="left")
        week_col = "adjclose" if current else "prev_close"
        start_col = "quarter_start" if naming == "quarter" else week_col
        simulation[f"market_{naming}ly_return"] = math.exp(np.log(market_return)/n)
        simulation[f"projected_{naming}ly_return"] = (simulation[f"price_prediction"] - simulation[start_col]) / simulation[start_col]
        simulation[f"{naming}ly_delta"] = [abs(x) for x in simulation[f"projected_{naming}ly_return"]]
        simulation[f"{naming}ly_delta_sign"] = [1 if x >= 0 else -1 for x in simulation[f"projected_{naming}ly_return"]]
        simulation[f"{naming}ly_rrr"] = simulation[f"{naming}ly_yield"] + simulation[f"{naming}ly_beta"] * (simulation[f"market_{naming}ly_return"] - simulation[f"{naming}ly_yield"]) - 1
        simulation = simulation.groupby(["year",naming,"day","ticker"]).mean().reset_index()
        return simulation