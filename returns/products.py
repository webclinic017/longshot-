import pandas as pd
from processor.processor import Processor as p
from datetime import timedelta
import math

## Mainly adhoc data products to support backtesting
class Products(object):

    @classmethod
    def spy_bench(self):
        bench = pd.read_csv("./csv_files/FED/SPY.csv")
        bench = p.column_date_processing(bench)
        bench["day"] = [x.weekday() for x in bench["date"]]
        bench["week"] = [x.week for x in bench["date"]]
        bench["year"] = [x.year for x in bench["date"]]
        bench["quarter"] = [x.quarter for x in bench["date"]]
        bench.rename(columns={"close/last":"adjclose"},inplace=True)
        bench_returns = bench.copy()
        bench_returns[f"bench_weekly_return"] = (bench_returns["adjclose"].shift(-1) - bench_returns["adjclose"]) / bench_returns["adjclose"]
        bench_quarterlies = bench_returns.groupby(["year","quarter"]).agg({"adjclose":"first"}).reset_index().rename(columns={"adjclose":"quarter_start"})
        bench_returns = bench_returns.merge(bench_quarterlies,on=["year","quarter"])
        bench_returns[f"bench_quarterly_return"] = (bench_returns["adjclose"].shift(-1) - bench_returns["quarter_start"]) / bench_returns["quarter_start"]
        bench_returns["weekly_variance"] = bench_returns["bench_weekly_return"].rolling(window=14).var()
        bench_returns["quarterly_variance"] = bench_returns["bench_quarterly_return"].rolling(window=14).var()
        bench_returns = bench_returns.dropna()
        return bench_returns

    @classmethod
    def tyields(self):
        tyields = pd.read_csv("./csv_files/FED/DGS1.csv")
        tyields = p.column_date_processing(tyields)
        tyields["dgs1"] = tyields["dgs1"].replace(".",0)
        tyields["dgs1"] = tyields["dgs1"].astype("float")
        tyields["yield"] = [1+(x/100) for x in tyields["dgs1"]]
        tyields["weekly_yield"] = [math.exp(math.log(x)/52) for x in tyields["yield"]]
        tyields["quarterly_yield"] = [math.exp(math.log(x)/4) for x in tyields["yield"]]
        tyields = tyields.dropna()
        return tyields