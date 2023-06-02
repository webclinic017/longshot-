from tqdm import tqdm
import pandas as pd
pd.options.mode.chained_assignment = None
from risk.quarterly_risk import QuarterlyRisk as risk_class
from returns.quarterly_returns import QuarterlyReturns as returns_class
from parameters.quarterly_parameters import QuarterlyParameters as qp
from strategy_filters.quarterly_filter import QuarterlyFilter as quarterly_filter_class
from backtester.abacktester import ABacktester
from processor.processor import Processor as p

## backtesting class to hold different backtesting methods
class QuarterlyBacktester(ABacktester):
    
    def __init__(self,strat_class,current,positions,start_date,end_date):
        super().__init__(strat_class,current,positions,start_date,end_date)
        self.returns = returns_class
        self.risk = risk_class


    def backtest(self,parameters,sim,sp500):
        dividend_tickers = list(sim["ticker"].unique())
        ranks = sim.merge(sp500[["ticker","GICS Sector"]],how="left").groupby(["year","quarter","GICS Sector"]).mean().reset_index().sort_values(["year","quarter","quarterly_return"],ascending=False).groupby(["year","quarter"]).first().reset_index().rename(columns={"GICS Sector":"top_sector"})[["year","quarter","top_sector"]]
        ranks["year"] = ranks["year"] + 1
        self.strat_class.db.connect()
        self.strat_class.db.drop("trades")
        for parameter in parameters:
            simulation = sim.copy()
            simulation = self.returns.returns_backtest(self.strat_class.name,simulation)
            simulation = quarterly_filter_class.strategy_filter(self.strat_class.name,simulation,sp500,ranks)
            self.backtest_helper(simulation.copy(),self.strat_class.positions,parameter,self.start_date,self.end_date,self.strat_class.db)
        self.strat_class.db.disconnect()

    def create_parameters(self):
        return qp.parameters()
    
    def create_sim(self,simulation,price_returns):
        sim = price_returns.merge(self.tyields[["year","quarter","quarterly_yield"]],on=["year","quarter"],how="left")
        colcol = [x for x in simulation.columns if self.strat_class.name in x] + ["year","quarter","ticker"]
        sim = sim.merge(simulation[colcol],on=["year","quarter","ticker"],how="left")
        sim = sim.dropna().groupby(["year","quarter","ticker"]).mean().reset_index()
        return sim
    
    def pull_sims(self):
        self.strat_class.db.connect()
        simulation = self.strat_class.db.retrieve("sim")
        self.strat_class.db.disconnect()
        simulation["year"] = simulation["year"] + 1
        return simulation
    
    def stock_returns(self,market,sec,sp500):
        new_prices = []
        market.connect()
        sec.connect()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        tickers = ["BTC"] if self.strat_class.asset_class == "crypto" else sp500["ticker"].unique()[:10]
        for ticker in tickers:
            try:
                ticker_sim = market.retrieve_ticker_prices(self.strat_class.asset_class,ticker)
                ticker_sim = p.column_date_processing(ticker_sim)
                if self.strat_class.asset_class == "prices":
                    cik = int(sp500[sp500["ticker"]==ticker]["CIK"])
                    financials = sec.retrieve_filing_data(cik)
                else:
                    financials = ticker_sim.groupby(["year","quarter","ticker"]).mean().reset_index()
                ticker_sim = self.returns.returns(ticker_sim,financials)
                completed = self.risk.risk(ticker_sim,self.bench_returns)
                new_prices.append(completed)
            except Exception as e:
                print(str(e))
                continue
        sec.disconnect()
        market.disconnect()
        price_returns = pd.concat(new_prices)
        return price_returns
    
    # risk oriented backtest utilizes quarterlies and quarterlies with additional floor, hedge and ceiling options
    def backtest_helper(self,sim,positions,parameter,start_date,end_date,db):
        ceiling = parameter["ceiling"]
        new_sim = []

        sim = sim[(sim["year"] >= start_date.year) & (sim["year"] <= end_date.year)]

        ## time based risk specific
        sim["risk_boolean"] = sim["quarterly_beta"] <= sim["quarterly_beta"].mean()
        sim["quarterly_return_boolean"] = sim["projected_quarterly_return"] > sim["quarterly_rrr"]
        sim["returns"] = [1 + row[1][f"quarterly_return"] for row in sim.iterrows()]
        new_sim.append(sim)
        ns = pd.concat(new_sim)
        test = ns[(ns["quarterly_return_boolean"]==True) & (ns["risk_boolean"]==True)]

        ## filters
        ## value component
        if parameter["value"] != True:
            test["quarterly_delta"] = test["quarterly_delta"] * -1

        ## ceiling component
        if ceiling:
            test = test[test["quarterly_return"]<=1]

        ledgers = []
        ## ledger creation
        ## strat specific 
        ## rank specific
        ## classification specific
        ## return specific 
        stuff = ["year","quarter","ticker","projected_quarterly_return","returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year","quarter","projected_quarterly_return"])[stuff].groupby(["year","quarter"],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()
        
        return_column = "returns"
        final["actual_returns"] = final[return_column]

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        db.store("trades",final)
    