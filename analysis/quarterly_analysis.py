from datetime import datetime

## Analysis transformations
class QuarterlyAnalysis(object):

    @classmethod
    def trade_analysis(self,indexer,ledger,positions,parameter,tyields,bench_returns):
        portfolio = ledger.pivot_table(index=["year","quarter"],columns="position",values="actual_returns").fillna(1).reset_index()
        counted_columns = [x for x in range(int(ledger["position"].max()+1))]
        for col in range(positions):
            if col not in counted_columns:
                portfolio[col] = 1
        counted_columns = [x for x in range(positions)]
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["date_string"] = [f'{int(row[1]["year"])}-Q{int(row[1]["quarter"])}' for row in portfolio.iterrows()]
        cumulative["date"] = [datetime.strptime(x + '-1', '%G-Q%V-%u') for x in cumulative["date_string"]]
        cumulative["pv"] = [sum([row[1][column] * 1/(2 ** (1+int(column))) for column in counted_columns]) for row in cumulative.iterrows()]
        cumulative = cumulative.merge(bench_returns[["date","adjclose","bench_return","variance"]],on="date",how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative["return"] = cumulative["pv"].pct_change().fillna(1)
        cumulative["beta"] = cumulative[["return","bench_return"]].cov().iloc[0][1]/cumulative["variance"].iloc[-1]
        cumulative["rrr"] = tyields["yield"].iloc[-1] + cumulative["beta"].iloc[-1]*(cumulative["bench"].iloc[-1]-tyields["yield"].iloc[-1])
        cumulative["sharpe"] = (cumulative["pv"] - tyields["yield"].iloc[-1]) / cumulative["beta"].iloc[-1]
        for index_stuff in indexer:
            cumulative[index_stuff] = parameter[index_stuff]
        return cumulative
        
    @classmethod
    def iteration_analysis(self,portfolio,positions,bench):
        counted_columns = [x for x in range(positions)]
        for i in counted_columns:
            if i not in portfolio.columns:
                portfolio[i] = 1
        cumulative = portfolio[[i for i in counted_columns]].cumprod()
        cumulative["date"] = [datetime(int(row[1]["year"]), 3*int(row[1]["quarter"]),1) for row in portfolio.iterrows()]
        cumulative["pv"] = [sum([row[1][column] * 1/(2 ** (1+int(column))) for column in counted_columns]) for row in cumulative.iterrows()]
        bench = bench.fillna(method="bfill")
        cumulative = cumulative.merge(bench[["date","adjclose"]],on="date",how="left")
        cumulative["bench"] = [1 + (row[1]["adjclose"] - cumulative["adjclose"].iloc[0]) / cumulative["adjclose"].iloc[0] for row in cumulative.iterrows()]
        cumulative = cumulative.fillna(method="bfill")
        return cumulative