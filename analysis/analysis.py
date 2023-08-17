from datetime import datetime, timedelta

## description: the analysis class for reporting and performance checking
class Analysis(object):

    def __init__(self,naming):
        self.naming = naming


    ## converting trade data into portfolio data and performance data
    def trade_analysis(self,ledger,positions):
        portfolio = ledger.pivot_table(index="date",columns="position",values="actual_returns").fillna(1).reset_index()
        counted_columns = [x for x in range(int(ledger["position"].max()+1))]
        for col in range(positions):
            if col not in counted_columns:
                portfolio[col] = 1
        counted_columns = [x for x in range(positions)]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative["date"] = portfolio["date"]
        # cumulative = cumulative.merge(bench_returns[["date","adjclose","bench_dately_return",f"{self.naming}ly_variance"]],on="date",how="left")
        # cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative["return"] = cumulative["pv"].pct_change().fillna(1)
        # cumulative["beta"] = cumulative[["return","bench_dately_return"]].cov().iloc[0][1]/cumulative[f"{self.naming}ly_variance"].iloc[-1]
        # cumulative["rrr"] = tyields[f"{self.naming}ly_yield10"].mean() + cumulative["beta"].mean()*(cumulative["bench"].mean()-tyields[f"{self.naming}ly_yield10"].mean())
        # cumulative["sharpe"] = (cumulative["pv"] - tyields[f"{self.naming}ly_yield10"].mean()) / cumulative["beta"].mean()
        return cumulative
    
    ## function to analyze the best iteration of a trade algorithms backtest
    def iteration_analysis(self,portfolio,positions):
        counted_columns = [x for x in range(positions)]
        # daily_returns = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in portfolio.iterrows()]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["date"] = portfolio["date"]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        # cumulative["daily_returns"] = daily_returns
        # bench = bench.fillna(method="bfill")
        # cumulative = cumulative.merge(bench[["date","adjclose"]],on="date",how="left")
        # cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative = cumulative.fillna(method="bfill")
        return cumulative