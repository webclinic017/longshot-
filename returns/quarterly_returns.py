from processor.processor import Processor as p
import math
import numpy as np
class QuarterlyReturns(object):

    ## helper function for weekly returns
    def calculate_quarterly_return(row):
        return row["return_end"] + (row["commonstockdividendspersharecashpaid"] / row["quarter_start"])
    
    @classmethod
    def returns(self,ticker_sim,financials):
        financials = p.column_date_processing(financials)
        ticker_sim = p.column_date_processing(ticker_sim)
        quarterlies = ticker_sim.groupby(["year","quarter","ticker"]).agg({"adjclose":"first"}).reset_index().rename(columns={"adjclose":"quarter_start"})
        end_quarterlies = ticker_sim.groupby(["year","quarter","ticker"]).agg({"adjclose":"last"}).reset_index().rename(columns={"adjclose":"quarter_end"})
        quarterlies = quarterlies.merge(end_quarterlies[["year","quarter","quarter_end"]],on=["year","quarter"],how="left")
        quarterlies["return_end"] = (quarterlies["quarter_end"] - quarterlies["quarter_start"]) / quarterlies["quarter_start"]
        if "commonstockdividendspersharecashpaid" in financials.columns:
            quarterlies = quarterlies.merge(financials[["year","quarter","commonstockdividendspersharecashpaid"]],how="left")
            quarterlies["commonstockdividendspersharecashpaid"] = quarterlies["commonstockdividendspersharecashpaid"].fillna(0)
        else:
            quarterlies["commonstockdividendspersharecashpaid"] = 0
        quarterly_returns = quarterlies.apply(self.calculate_quarterly_return, axis=1)
        quarterlies.loc[:, "quarterly_return"] = quarterly_returns
        return quarterlies
    
    @classmethod
    def returns_backtest(self,name,simulation):
        simulation["market_quarterly_return"] = math.exp(np.log(1.15)/4)
        # simulation = simulation[simulation["ticker"].isin(dividend_tickers)]
        simulation["projected_quarterly_return"] = (simulation[f"{name}_prediction"] - simulation["quarter_start"]) / simulation["quarter_start"]
        simulation["quarterly_delta"] = [abs(x) for x in simulation["projected_quarterly_return"]]
        simulation["quarterly_delta_sign"] = [1 if x >= 0 else -1 for x in simulation["projected_quarterly_return"]]
        simulation["quarterly_rrr"] = simulation["quarterly_yield"] + simulation["quarterly_beta"] * (simulation["market_quarterly_return"] - simulation["quarterly_yield"]) - 1
        return simulation