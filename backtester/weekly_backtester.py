from tqdm import tqdm
import pandas as pd
pd.options.mode.chained_assignment = None

from risk.weekly_risk import WeeklyRisk as risk_class
from returns.weekly_returns import WeeklyReturns as returns_class
from backtester.abacktester import ABacktester
from risk.weekly_risk import WeeklyRisk as risk_class
from returns.weekly_returns import WeeklyReturns as returns_class
from parameters.weekly_parameters import WeeklyParameters as wp

## backtesting class to hold different backtesting methods
class WeeklyBacktester(ABacktester):

    def __init__(self,strat_class,current,positions,start_date,end_date):
        super().__init__(strat_class,current,positions,start_date,end_date)
        self.returns = returns_class
        self.risk = risk_class
    
    def backtest(self,parameters,sim,sp500):
        self.strat_class.db.connect()
        self.strat_class.db.drop("trades")
        backtest_data = sim.copy().dropna()
        backtest_data = returns_class.returns_backtest(self.strat_class.name,backtest_data)
        for parameter in parameters:
            self.backtest_helper(backtest_data.copy(),self.strat_class.positions,parameter,self.start_date,self.end_date,self.strat_class.db)
        self.strat_class.db.disconnect()

    def create_parameters(self):
        return wp.parameters()
    
    def stock_returns(self,market,sec,sp500):
        new_prices = []
        market.connect()
        sp500 = sp500.rename(columns={"Symbol":"ticker"})
        tickers = ["BTC"] if self.strat_class.asset_class == "crypto" else sp500["ticker"].unique()[:10]
        for ticker in tickers:
            try:
                ticker_sim = market.retrieve_ticker_prices(self.strat_class.asset_class,ticker)
                ticker_sim = self.returns.returns(ticker_sim)
                completed = self.risk.risk(ticker_sim,self.bench_returns)
                new_prices.append(completed)
            except Exception as e:
                print(str(e))
                continue
        market.disconnect()
        price_returns = pd.concat(new_prices)
        return price_returns

    # risk oriented backtest utilizes weeklies with additional floor, and ceiling options includes shorts
    def backtest_helper(self,sim,positions,parameter,start_date,end_date,db):
        classification = parameter["classification"]
        ceiling = parameter["ceiling"]
        floor = parameter["floor"]
        floor_value = -0.05
        new_sim = []

        sim = sim[(sim["date"] >= start_date) & (sim["date"] <= end_date)]
        ## optimizing
        sim["date_boolean"] = [x.weekday() == 0 for x in sim["date"]]
        sim = sim[sim["date_boolean"]==True]
        sim["risk_boolean"] = sim["beta"] <= sim["beta"].mean()
        sim["return_boolean"] = sim["delta"] > sim["rrr"]
        sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
        sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
        sim["floor_boolean"] = 4 >= sim["floor_index"]
        sim["returns"] = [1 + row[1][f"return_{4}"] * row[1]["delta_sign"] for row in  sim.iterrows()]
        sim["floored_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["returns"] for row in sim.iterrows()]

        new_sim.append(sim)
        ns = pd.concat(new_sim)
        test = ns[(ns["return_boolean"]==True) & (ns["risk_boolean"]==True)]

        ## filters
        if parameter["value"] != True:
            test["delta"] = test["delta"] * -1
            if "classification_prediction" in test.columns:
                test["classification_prediction"] = [not x for x in test["classification_prediction"]]

        if classification and "classification_prediction" in test.columns:
            test = test[test["classification_prediction"]==True]
        if ceiling:
            test = test[test["delta"]<=1]

        ledgers = []
        ## ledger creation
        stuff = ["year","week","ticker","delta_sign","delta","returns","floored_returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year","week","delta"])[stuff].groupby(["year","week"],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()

        if floor:
            return_column = "floored_returns"
        else:
            return_column = "returns"
        final["actual_returns"] = final[return_column]

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        db.store("trades",final)