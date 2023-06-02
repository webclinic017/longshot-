import math
import numpy as np
from processor.processor import Processor as p
import pandas as pd
from database.market import Market
from tqdm import tqdm

class WeeklyReturns(object):
    
    @classmethod
    def returns(self,ticker_sim):
        ticker_sim = p.column_date_processing(ticker_sim)
        for i in range(2,5):
            ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"].shift(-1)) / ticker_sim["adjclose"].shift(-1)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        ticker_sim["weekly_return"] = ticker_sim["return_4"]
        return ticker_sim
    
    @classmethod
    def returns_backtest(self,name,backtest_data):
        backtest_data["projected_return"] = (backtest_data[f"{name}_prediction"] - backtest_data["adjclose"]) / backtest_data["adjclose"]
        backtest_data["delta"] = [abs(x) for x in backtest_data["projected_return"]]
        backtest_data["delta_sign"] = [1 if x >= 0 else -1 for x in backtest_data["projected_return"]]
        backtest_data["market_return"] = math.exp(np.log(1.15)/52)
        backtest_data["rrr"] = backtest_data["weekly_yield"] + backtest_data["beta"] * (backtest_data["market_return"] - backtest_data["weekly_yield"]) - 1
        backtest_data = backtest_data.groupby(["date","ticker"]).mean().reset_index()
        backtest_data.sort_values("date",inplace=True)
        return backtest_data