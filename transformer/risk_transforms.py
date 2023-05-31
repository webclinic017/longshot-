from processor.processor import Processor as p
import numpy as np
import math
import pandas as pd
pd.options.mode.chained_assignment = None

## Risk transformer to convert returns to variances and other risk metrics
class Risk(object):


    ## last step in bitcoin backtest data prep providing required returns
    @classmethod
    def btc_backtest_prep(self,strat,backtest_data):
        if strat == "spec":
            backtest_data["projected_return"] = (backtest_data[f"prediction"] - backtest_data["adjclose"]) / backtest_data["adjclose"]
        else:
            backtest_data["projected_return"] = (backtest_data[f"{strat}_prediction"] - backtest_data["adjclose"]) / backtest_data["adjclose"]
        backtest_data["delta"] = [abs(x) for x in backtest_data["projected_return"]]
        backtest_data["delta_sign"] = [1 if x >= 0 else -1 for x in backtest_data["projected_return"]]
        backtest_data["market_return"] = math.exp(np.log(1.15)/52)
        backtest_data["market_quarterly_return"] = math.exp(np.log(1.15)/4)
        backtest_data["rrr"] = backtest_data["weekly_yield"] + backtest_data["beta"] * (backtest_data["market_return"] - backtest_data["weekly_yield"]) - 1
        backtest_data = backtest_data.groupby(["date","ticker"]).mean().reset_index()
        backtest_data.sort_values("date",inplace=True)
        return backtest_data

    ## first step in backtest data prep providing different return strats and beta values
    @classmethod
    def btc_risk_prep(self,ticker_sim,bench_returns):
        for i in range(2,5):
            ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"].shift(-1)) / ticker_sim["adjclose"].shift(-1)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        ticker_sim["window_prediction"] = ticker_sim["adjclose"].shift(6)
        ticker_sim["rolling_prediction"] = ticker_sim["adjclose"].rolling(window=100).mean()
        returns = ticker_sim.copy()
        returns["weekly_return"] = returns["return_4"]
        new_sim = ticker_sim.merge(returns[["year","week","weekly_return"]], on=["year","week"],how="left") \
                            .merge(bench_returns[["year","week","bench_return","variance"]],on=["year","week"],how="left")
        new_sim["market_cov"] = new_sim["weekly_return"].rolling(window=100).cov(new_sim["bench_return"])
        completed = new_sim.copy()
        completed["beta"] = completed["market_cov"] / completed["variance"]
        completed = completed[completed["day"]==0]
        completed  = completed.groupby(["date","ticker"]).mean().reset_index()
        return completed
    
    ## helper function for weekly returns
    def calculate_weekly_return(row):
        if row["week"] % 13 != 2:
            return row["return_4"]
        else:
            return row["return_4"] + (row["commonstockdividendspersharecashpaid"] / row["adjclose"])
    
    ## stock backtesting risk product transformations
    @classmethod
    def backtesting_risk_prep(self,ticker_sim,bench_returns,financials):
        financials = p.column_date_processing(financials)
        ticker_sim = p.column_date_processing(ticker_sim)
        for i in range(2,5):
            ticker_sim[f"return_{i}"] = (ticker_sim["adjclose"].shift(-i) - ticker_sim["adjclose"].shift(-1)) / ticker_sim["adjclose"].shift(-1)
        quarterlies = ticker_sim.groupby(["year","quarter"]).agg({"adjclose":"first"}).reset_index().rename(columns={"adjclose":"quarter_start"})
        end_quarterlies = ticker_sim.groupby(["year","quarter"]).agg({"adjclose":"last"}).reset_index().rename(columns={"adjclose":"quarter_end"})
        quarterlies = quarterlies.merge(end_quarterlies[["year","quarter","quarter_end"]],on=["year","quarter"],how="left")
        quarterlies["qturn"] = (quarterlies["quarter_end"] - quarterlies["quarter_start"]) / quarterlies["quarter_start"]
        if "commonstockdividendspersharecashpaid" in financials.columns:
            quarterlies = quarterlies.merge(financials[["year","quarter","commonstockdividendspersharecashpaid"]],how="left")
            quarterlies["commonstockdividendspersharecashpaid"] = quarterlies["commonstockdividendspersharecashpaid"].fillna(0)
        else:
            quarterlies["commonstockdividendspersharecashpaid"] = 0
        ticker_sim = ticker_sim.merge(quarterlies,on=["year","quarter"],how="left")
        ticker_sim["quarterly_return"] =  (ticker_sim["adjclose"].shift(-1) - ticker_sim["quarter_start"]) / ticker_sim["quarter_start"]
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        returns = ticker_sim.copy()
        weekly_returns = returns.apply(self.calculate_weekly_return, axis=1)
        returns.loc[:, "weekly_return"] = weekly_returns
        new_sim = ticker_sim.merge(returns[["year","week","weekly_return"]], on=["year","week"],how="left") \
                            .merge(bench_returns[["year","week","bench_return","variance","bench_quarterly_return","quarterly_variance"]],on=["year","week"],how="left")
        new_sim = new_sim.dropna()
        new_sim["market_cov"] = new_sim["weekly_return"].rolling(window=14).cov(new_sim["bench_return"])
        new_sim["market_quarterly_cov"] = new_sim["quarterly_return"].rolling(window=4).cov(new_sim["bench_quarterly_return"])
        completed = new_sim.copy()
        completed["beta"] = completed["market_cov"] / completed["variance"]
        completed["quarterly_beta"] = completed["market_quarterly_cov"] / completed["quarterly_variance"]
        completed  = completed.dropna()
        return completed
    
    ## mid step in stock backtesting prep providing required weekly returns
    @classmethod
    def required_returns(self,sim):
        sim["projected_return"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
        sim["delta"] = [abs(x) for x in sim["projected_return"]]
        sim["delta_sign"] = [1 if x >= 0 else -1 for x in sim["projected_return"]]
        sim["market_return"] = math.exp(np.log(1.15)/52)
        sim["market_quarterly_return"] = math.exp(np.log(1.15)/4)
        sim["rrr"] = sim["weekly_yield"] + sim["beta"] * (sim["market_return"] - sim["weekly_yield"]) - 1
        return sim
    
    ## last step in stock backtesting prep providing strat specific transformations
    @classmethod
    def strat_specific(self,strat,simulation,dividend_tickers,sp500,ranks):
        if strat == "dividends":
            simulation = simulation[simulation["ticker"].isin(dividend_tickers)]
            simulation["projected_quarterly_return"] = (simulation[f"dividends_prediction"] - simulation["adjclose"]) / simulation["adjclose"]
        else:
            simulation["projected_quarterly_return"] = (simulation[f"financial_prediction"] - simulation["adjclose"]) / simulation["adjclose"]
        simulation["quarterly_delta"] = [abs(x) for x in simulation["projected_quarterly_return"]]
        simulation["quarterly_delta_sign"] = [1 if x >= 0 else -1 for x in simulation["projected_quarterly_return"]]
        simulation["quarterly_rrr"] = simulation["quarterly_yield"] + simulation["quarterly_beta"] * (simulation["market_quarterly_return"] - simulation["quarterly_yield"]) - 1
        simulation["projected_pe"] = simulation["financial_prediction"] / simulation["earnings_prediction"]
        if strat == "earnings":
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
        