from processor.processor import Processor as p
import math
from datetime import timedelta

## Mainly adhoc data products to support backtesting
class Products(object):

    @classmethod
    def spy_bench(self,bench,time_horizon_class):
        bench = p.column_date_processing(bench)
        bench.rename(columns={"value":"adjopen"},inplace=True)
        bench_returns = bench.copy().sort_values("date")
        bench_returns[f"bench_return"] = (bench_returns["adjopen"] - bench_returns["adjopen"].shift(time_horizon_class.window)) / bench_returns["adjopen"].shift(time_horizon_class.window)
        bench_returns["variance"] = bench_returns["bench_return"].rolling(window=time_horizon_class.rolling).var()
        bench_returns["date"] = [x + timedelta(days=7) for x in bench_returns["date"]]
        bench_returns = bench_returns.dropna()
        return bench_returns

    @classmethod
    def tyields(self,tyields,maturity,time_horizon_class):
        tyields = p.column_date_processing(tyields)
        tyields[f"yield{maturity}"] = [math.exp(math.log(1+(x/100))/time_horizon_class.instances_per_year) for x in tyields["value"]]
        tyields["date"] = [x + timedelta(days=7) for x in tyields["date"]]
        tyields = tyields.dropna()
        return tyields