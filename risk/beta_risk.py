class BetaRisk(object):
    
    def __init__(self):
        self.name = "BETA"
    
    def risk(self,atime_horizon,ticker_sim,bench_returns):
        new_sim = ticker_sim.merge(bench_returns[["date",f"bench_return",f"variance"]],on="date",how="left")
        new_sim[f"market_cov"] = new_sim[f"risk_return"].rolling(window=atime_horizon.rolling).cov(new_sim[f"bench_return"])
        completed = new_sim.copy()
        completed[f"beta"] = completed[f"market_cov"] / completed[f"variance"]
        completed.dropna(inplace=True)
        return completed.groupby(["year","quarter","month","week","date","ticker"]).mean().reset_index()