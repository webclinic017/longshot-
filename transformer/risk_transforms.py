from processor.processor import Processor as p
import numpy as np
import math
import pandas as pd
pd.options.mode.chained_assignment = None

## Risk transformer to convert returns to variances and other risk metrics
class Risk(object):

    ## helper function for weekly returns
    def calculate_quarterly_return(row):
        return row["return_end"] + (row["commonstockdividendspersharecashpaid"] / row["quarter_start"])
    
    ## stock backtesting risk product transformations
    @classmethod
    def quarterly_risk_prep(self,ticker_sim,bench_returns,financials):
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
        new_sim = quarterlies.merge(bench_returns[["year","quarter","bench_quarterly_return","quarterly_variance"]],on=["year","quarter"],how="left")
        new_sim = new_sim.dropna()
        new_sim["market_quarterly_cov"] = new_sim["quarterly_return"].rolling(window=4).cov(new_sim["bench_quarterly_return"])
        new_sim["quarterly_beta"] = new_sim["market_quarterly_cov"] / new_sim["quarterly_variance"]
        new_sim.dropna(inplace=True)
        return new_sim

    @classmethod
    def weekly_risk_prep(self,ticker_sim,bench_returns):
        ticker_sim = p.column_date_processing(ticker_sim)
        for i in range(2,5):
            ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"].shift(-1)) / ticker_sim["adjclose"].shift(-1)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        ticker_sim["weekly_return"] = ticker_sim["return_4"]
        ticker_sim["rolling_prediction"] = ticker_sim["adjclose"].rolling(100).mean()
        ticker_sim["window_prediction"] = ticker_sim["adjclose"].shift(10)
        new_sim = ticker_sim.merge(bench_returns[["year","week","bench_return","variance"]],on=["year","week"],how="left")
        new_sim["market_cov"] = new_sim["weekly_return"].rolling(window=14).cov(new_sim["bench_return"])
        completed = new_sim.copy()
        completed["beta"] = completed["market_cov"] / completed["variance"]
        completed.dropna(inplace=True)
        return completed
    
    ## last step in bitcoin backtest data prep providing required returns
    @classmethod
    def weekly_backtest_prep(self,strat,backtest_data):
        if strat == "spec":
            backtest_data["projected_return"] = (backtest_data[f"prediction"] - backtest_data["adjclose"]) / backtest_data["adjclose"]
        else:
            backtest_data["projected_return"] = (backtest_data[f"{strat}_prediction"] - backtest_data["adjclose"]) / backtest_data["adjclose"]
        backtest_data["delta"] = [abs(x) for x in backtest_data["projected_return"]]
        backtest_data["delta_sign"] = [1 if x >= 0 else -1 for x in backtest_data["projected_return"]]
        backtest_data["market_return"] = math.exp(np.log(1.15)/52)
        backtest_data["rrr"] = backtest_data["weekly_yield"] + backtest_data["beta"] * (backtest_data["market_return"] - backtest_data["weekly_yield"]) - 1
        backtest_data = backtest_data.groupby(["date","ticker"]).mean().reset_index()
        backtest_data.sort_values("date",inplace=True)
        return backtest_data
        
    ## last step in stock backtesting prep providing strat specific transformations
    @classmethod
    def quarterly_backtest_prep(self,strat,simulation,dividend_tickers,sp500,ranks):
        simulation["market_quarterly_return"] = math.exp(np.log(1.15)/4)
        if strat == "dividends":
            simulation = simulation[simulation["ticker"].isin(dividend_tickers)]
            simulation["projected_quarterly_return"] = (simulation[f"dividends_prediction"] - simulation["quarter_start"]) / simulation["quarter_start"]
        else:
            simulation["projected_quarterly_return"] = (simulation[f"financial_prediction"] - simulation["quarter_start"]) / simulation["quarter_start"]
        simulation["quarterly_delta"] = [abs(x) for x in simulation["projected_quarterly_return"]]
        simulation["quarterly_delta_sign"] = [1 if x >= 0 else -1 for x in simulation["projected_quarterly_return"]]
        simulation["quarterly_rrr"] = simulation["quarterly_yield"] + simulation["quarterly_beta"] * (simulation["market_quarterly_return"] - simulation["quarterly_yield"]) - 1
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
        