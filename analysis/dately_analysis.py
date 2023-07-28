from datetime import datetime, timedelta

## Analysis transformations
class DatelyAnalysis(object):

    ## converting trade data into portfolio data and performance data
    @classmethod
    def trade_analysis(self,ledger,positions,tyields,bench_returns):
        portfolio = ledger.pivot_table(index=["year","date"],columns="position",values="actual_returns").fillna(1).reset_index()
        counted_columns = [x for x in range(int(ledger["position"].max()+1))]
        for col in range(positions):
            if col not in counted_columns:
                portfolio[col] = 1
        counted_columns = [x for x in range(positions)]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["year"] = portfolio["year"]
        cumulative["date"] = portfolio["date"]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative = cumulative.merge(bench_returns[["year","date","adjclose","bench_dately_return","dately_variance"]],on=["year","date"],how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative["return"] = cumulative["pv"].pct_change().fillna(1)
        cumulative["beta"] = cumulative[["return","bench_dately_return"]].cov().iloc[0][1]/cumulative["dately_variance"].iloc[-1]
        cumulative["rrr"] = tyields["dately_yield"].mean() + cumulative["beta"].mean()*(cumulative["bench"].mean()-tyields["dately_yield"].mean())
        cumulative["sharpe"] = (cumulative["pv"] - tyields["dately_yield"].mean()) / cumulative["beta"].mean()
        return cumulative
    
    @classmethod
    def iteration_analysis(self,portfolio,positions,bench):
        counted_columns = [x for x in range(positions)]
        daily_returns = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in portfolio.iterrows()]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["year"] = portfolio["year"]
        cumulative["date"] = portfolio["date"]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative["daily_returns"] = daily_returns
        bench = bench.fillna(method="bfill")
        cumulative = cumulative.merge(bench[["year","date","adjclose"]],on=["year","date"],how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative = cumulative.fillna(method="bfill")
        return cumulative