from datetime import datetime, timedelta

## description: the analysis class for reporting and performance checking
class Analysis(object):

    def __init__(self,naming):
        self.naming = naming


    ## converting trade data into portfolio data and performance data
    def trade_analysis(self,ledger,positions,tyields,bench_returns):
        portfolio = ledger.pivot_table(index=["year",self.naming],columns="position",values="actual_returns").fillna(1).reset_index()
        counted_columns = [x for x in range(int(ledger["position"].max()+1))]
        for col in range(positions):
            if col not in counted_columns:
                portfolio[col] = 1
        counted_columns = [x for x in range(positions)]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["year"] = portfolio["year"]
        cumulative[self.naming] = portfolio[self.naming]
        if self.naming == "month":
            cumulative["date"] = [datetime(int(row[1]["year"]),int(row[1]["month"]),28) for row in cumulative.iterrows()]
        if self.naming != "week":
            cumulative["week"] = [x.week for x in cumulative["date"]]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative = cumulative.merge(bench_returns[["year","week","adjclose","bench_dately_return",f"{self.naming}ly_variance"]],on=["year","week"],how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative["return"] = cumulative["pv"].pct_change().fillna(1)
        cumulative["beta"] = cumulative[["return","bench_dately_return"]].cov().iloc[0][1]/cumulative[f"{self.naming}ly_variance"].iloc[-1]
        cumulative["rrr"] = tyields[f"{self.naming}ly_yield10"].mean() + cumulative["beta"].mean()*(cumulative["bench"].mean()-tyields[f"{self.naming}ly_yield10"].mean())
        cumulative["sharpe"] = (cumulative["pv"] - tyields[f"{self.naming}ly_yield10"].mean()) / cumulative["beta"].mean()
        return cumulative
    
    ## function to analyze the best iteration of a trade algorithms backtest
    def iteration_analysis(self,portfolio,positions,bench):
        counted_columns = [x for x in range(positions)]
        daily_returns = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in portfolio.iterrows()]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["year"] = portfolio["year"]
        cumulative[self.naming] = portfolio[self.naming]
        if self.naming == "date":
            cumulative["week"] = [x.week for x in cumulative["date"]]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative["daily_returns"] = daily_returns
        bench = bench.fillna(method="bfill")
        cumulative = cumulative.merge(bench[["year",self.naming,"adjclose"]],on=["year",self.naming],how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        if self.naming == "week":
            cumulative["date_string"] = [f'{int(row[1]["year"])}-W{int(row[1]["week"])}' for row in cumulative.iterrows()]
            cumulative["date"] = [datetime.strptime(x + '-1', '%G-W%V-%u') + timedelta(days=4) for x in cumulative["date_string"]]
        if self.naming == "month":
            cumulative["date"] = [datetime(int(row[1]["year"]),int(row[1]["month"]),28) for row in cumulative.iterrows()]
        cumulative = cumulative.fillna(method="bfill")
        return cumulative