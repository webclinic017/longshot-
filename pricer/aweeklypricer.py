from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from pricer.aipricer import AIPricer


class AWeeklyPricer(AIPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.factors = [str(x) for x in range(14)]
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        self.positions = 100 if asset_class.value == "stocks" else 1
        
    def price_returns(self,ticker_sim,current):
        ticker_sim = p.column_date_processing(ticker_sim)
        ticker_sim = ticker_sim.sort_values("date")
        ticker_sim["prev_close"] = ticker_sim["adjclose"].shift(1)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        if not current:
            for i in [5,10,15,20]:
                ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"]) / ticker_sim["adjclose"]
        ticker_sim["weekly_risk_return"] = (ticker_sim["adjclose"].shift(1) - ticker_sim["adjclose"].shift(6)) / ticker_sim["adjclose"].shift(6)
        return ticker_sim