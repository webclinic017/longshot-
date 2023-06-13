from datetime import datetime, timedelta

## Analysis transformations
class WeeklyAnalysis(object):

    ## converting trade data into portfolio data and performance data
    @classmethod
    def trade_analysis(self,indexer,ledger,positions,parameter,tyields,bench_returns):
        portfolio = ledger.pivot_table(index=["year","week"],columns="position",values="actual_returns").fillna(1).reset_index()
        counted_columns = [x for x in range(int(ledger["position"].max()+1))]
        for col in range(positions):
            if col not in counted_columns:
                portfolio[col] = 1
        counted_columns = [x for x in range(positions)]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["year"] = portfolio["year"]
        cumulative["week"] = portfolio["week"]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative = cumulative.merge(bench_returns[["year","week","adjclose","bench_weekly_return","weekly_variance"]],on=["year","week"],how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative["return"] = cumulative["pv"].pct_change().fillna(1)
        cumulative["beta"] = cumulative[["return","bench_weekly_return"]].cov().iloc[0][1]/cumulative["weekly_variance"].iloc[-1]
        cumulative["rrr"] = tyields["weekly_yield"].iloc[-1] + cumulative["beta"].iloc[-1]*(cumulative["bench"].iloc[-1]-tyields["weekly_yield"].iloc[-1])
        cumulative["sharpe"] = (cumulative["pv"] - tyields["weekly_yield"].iloc[-1]) / cumulative["beta"].iloc[-1]
        for index_stuff in indexer:
            cumulative[index_stuff] = parameter[index_stuff]
        return cumulative
    
    @classmethod
    def iteration_analysis(self,portfolio,positions,bench):
        counted_columns = [x for x in range(positions)]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["year"] = portfolio["year"]
        cumulative["week"] = portfolio["week"]
        cumulative["pv"] = [sum([row[1][column] * 1/positions for column in counted_columns]) for row in cumulative.iterrows()]
        bench = bench.fillna(method="bfill")
        cumulative = cumulative.merge(bench[["year","week","adjclose"]],on=["year","week"],how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative["date_string"] = [f'{int(row[1]["year"])}-W{int(row[1]["week"])}' for row in cumulative.iterrows()]
        cumulative["date"] = [datetime.strptime(x + '-1', '%G-W%V-%u') + timedelta(days=4) for x in cumulative["date_string"]]
        cumulative = cumulative.fillna(method="bfill")
        return cumulative