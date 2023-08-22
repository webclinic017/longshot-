import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

class Backtester(object):
    
    @classmethod
    def backtest(self,iteration_sim,iteration,parameter,current):

        strategy = parameter["strategy"]
        lookback = parameter["lookback"]
        volatility = parameter["volatility"]
        value = parameter["value"]
        positions = len(list(iteration_sim["GICS Sector"].unique()))
        
        iteration_sim["signal"] = (iteration_sim[f"{strategy}_{lookback}"] - iteration_sim["prev_close"]) / iteration_sim["prev_close"]
        iteration_sim = iteration_sim[iteration_sim[f"rolling_pct_stdev_{lookback}"]<=volatility]
        iteration_sim = iteration_sim[iteration_sim["day"]<4]
        
        if not value:
            iteration_sim["signal"] = iteration_sim["signal"] * -1

        iteration_sim = iteration_sim.sort_values(["date","signal"],ascending=False).groupby(["date","GICS Sector"]).first().reset_index()   
        iteration_sim = iteration_sim[(iteration_sim["signal"]>-1) & (iteration_sim["signal"]<1)]
        
        additional_columns = ["signal"]
        
        if not current:
            additional_columns.append("return")
        
        trades = iteration_sim.sort_values(["date","signal"]).groupby("date").nth([-1-i for i in range(positions)]).reset_index()[["date","ticker"]+additional_columns]
        trades["position"] = [int(x % positions) for x in trades.index.values]
        trades["iteration"] = iteration
        for key in parameter.keys():
            trades[key] = parameter[key]

        return trades