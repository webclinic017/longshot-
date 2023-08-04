class BetaRisk(object):
    
    def __init__(self):
        self.name = "BETA"
    
    def risk(self,atime_horizon,ticker_sim,bench_returns):
        naming = atime_horizon.naming_convention
        new_sim = ticker_sim.merge(bench_returns[["year","week",f"bench_{naming}ly_return",f"{naming}ly_variance"]],on=["year","week"],how="left")
        new_sim[f"market_{naming}ly_cov"] = new_sim[f"{naming}ly_risk_return"].rolling(window=100).cov(new_sim[f"bench_{naming}ly_return"])
        completed = new_sim.copy()
        completed[f"{naming}ly_beta"] = completed[f"market_{naming}ly_cov"] / completed[f"{naming}ly_variance"]
        completed.dropna(inplace=True)
        return completed.groupby(["year","quarter","month","week","date","ticker"]).mean().reset_index()