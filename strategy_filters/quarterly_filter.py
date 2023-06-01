import pandas as pd
class QuarterlyFilter(object):

    @classmethod
    def strategy_filter(self,strat,simulation,sp500,ranks):
        if strat == "earnings":
            simulation["projected_pe"] = simulation["financial_prediction"] / simulation["earnings_prediction"]
            industry_filter = []
            for industry in sp500["GICS Sector"].unique():
                tickers = list(sp500[sp500["GICS Sector"]==industry]["ticker"])
                industry_simulation = simulation[simulation["ticker"].isin(tickers)]
                filtered = industry_simulation[(industry_simulation["projected_pe"] <= industry_simulation["projected_pe"].mean()) & (industry_simulation["projected_pe"] > 0)]
                industry_filter.append(filtered)
            simulation = pd.concat(industry_filter)
        if strat == "sector":
            simulation = simulation.merge(ranks,on=["year","quarter"],how="left").merge(sp500[["ticker","GICS Sector"]],how="left")
            simulation["go_industry"] = simulation["GICS Sector"] == simulation["top_sector"]
            simulation = simulation[simulation["go_industry"]==True]
        return simulation