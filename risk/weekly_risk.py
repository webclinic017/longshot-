

class WeeklyRisk(object):

    @classmethod
    def risk(self,ticker_sim,bench_returns):
        new_sim = ticker_sim.merge(bench_returns[["year","week","bench_return","variance"]],on=["year","week"],how="left")
        new_sim["market_cov"] = new_sim["weekly_return"].rolling(window=14).cov(new_sim["bench_return"])
        completed = new_sim.copy()
        completed["beta"] = completed["market_cov"] / completed["variance"]
        completed.dropna(inplace=True)
        return completed