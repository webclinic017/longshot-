

class QuarterlyRisk(object):

    @classmethod
    def risk(self,quarterlies,bench_returns):
        new_sim = quarterlies.merge(bench_returns[["year","quarter","bench_quarterly_return","quarterly_variance"]],on=["year","quarter"],how="left")
        new_sim = new_sim.dropna()
        new_sim["market_quarterly_cov"] = new_sim["quarterly_return"].rolling(window=4).cov(new_sim["bench_quarterly_return"])
        new_sim["quarterly_beta"] = new_sim["market_quarterly_cov"] / new_sim["quarterly_variance"]
        new_sim.dropna(inplace=True)
        return new_sim