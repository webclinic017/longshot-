import pandas as pd
from processor.processor import Processor as p
from datetime import timedelta
import math
from extractor.fred_extractor import FREDExtractor
from datetime import datetime, timedelta
import pandas as pd
from database.market import Market
## Mainly adhoc data products to support backtesting
class Products(object):

    @classmethod
    def spy_bench(self,bench):
        bench = p.column_date_processing(bench)
        bench.rename(columns={"value":"adjclose"},inplace=True)
        bench_returns = bench.copy().sort_values("date")
        bench_returns[f"bench_dately_return"] = (bench_returns["adjclose"] - bench_returns["adjclose"].shift(1)) / bench_returns["adjclose"].shift(1)
        bench_weekly_returns = bench.copy().sort_values("date").groupby(["year","quarter","week"]).mean().reset_index()
        bench_weekly_returns[f"bench_weekly_return"] = (bench_returns["adjclose"] - bench_returns["adjclose"].shift(1)) / bench_returns["adjclose"].shift(1)
        bench_quarterlies = bench.copy().groupby(["year","quarter"]).agg({"adjclose":"first"}).reset_index().rename(columns={"adjclose":"quarter_start"})
        bench_monthlies = bench.copy().groupby(["year","month"]).agg({"adjclose":"first"}).reset_index().rename(columns={"adjclose":"month_start"})
        bench_returns = bench_returns.merge(bench_weekly_returns[["year","quarter","week","bench_weekly_return"]],on=["year","quarter","week"])
        bench_returns = bench_returns.merge(bench_quarterlies,on=["year","quarter"])
        bench_returns = bench_returns.merge(bench_monthlies,on=["year","month"])
        bench_returns[f"bench_quarterly_return"] = (bench_returns["adjclose"] - bench_returns["quarter_start"]) / bench_returns["quarter_start"]
        bench_returns[f"bench_monthly_return"] = (bench_returns["adjclose"] - bench_returns["month_start"]) / bench_returns["month_start"]
        bench_returns["dately_variance"] = bench_returns["bench_dately_return"].rolling(window=100).var()
        bench_returns["weekly_variance"] = bench_returns["bench_weekly_return"].rolling(window=14).var()
        bench_returns["monthly_variance"] = bench_returns["bench_monthly_return"].rolling(window=14).var()
        bench_returns["quarterly_variance"] = bench_returns["bench_quarterly_return"].rolling(window=14).var()
        bench_returns = bench_returns.groupby(["year","quarter","week","date"]).mean().reset_index()
        bench_returns["date"] = bench_returns["date"] + timedelta(days=7)
        # bench_returns["week"] = bench_returns["week"] + 1
        bench_returns = bench_returns.dropna()
        return bench_returns

    @classmethod
    def tyields(self,tyields,maturity):
        tyields = p.column_date_processing(tyields)
        tyields[f"yield{maturity}"] = [1+(x/100) for x in tyields["value"]]
        tyields[f"dately_yield{maturity}"] = [math.exp(math.log(x)/365) for x in tyields[f"yield{maturity}"]]
        tyields[f"weekly_yield{maturity}"] = [math.exp(math.log(x)/52) for x in tyields[f"yield{maturity}"]]
        tyields[f"monthly_yield{maturity}"] = [math.exp(math.log(x)/12) for x in tyields[f"yield{maturity}"]]
        tyields[f"quarterly_yield{maturity}"] = [math.exp(math.log(x)/4) for x in tyields[f"yield{maturity}"]]
        tyields = tyields.groupby(["year","quarter","week","date"]).mean().reset_index()
        tyields[f"date"] = tyields["date"] + timedelta(days=7)
        # tyields[f"week"] = tyields["week"] + 1
        tyields = tyields.dropna()
        return tyields