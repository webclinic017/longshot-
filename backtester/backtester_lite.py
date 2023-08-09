
class BacktesterLite(object):
    
    @classmethod
    def backtest(self,positions,final,iteration,parameter,current):
        iteration_sim = final.copy()
        strategy = parameter["strategy"]
        lookback = parameter["lookback"]
        iteration_sim["signal"] = (iteration_sim[f"{strategy}_{lookback}"] - iteration_sim["prev_close"]) / iteration_sim["prev_close"] 
        if not parameter["value"]:
            iteration_sim["signal"] = iteration_sim["signal"] * -1
        if not current:
            additional_column = "return"
        else:
            additional_column = "signal"
        trades = iteration_sim.sort_values(["date","signal"]).groupby("date",as_index=True).nth([-1-i for i in range(positions)]).reset_index()[["date","ticker",additional_column]]
        trades["position"] = [int(x % positions) for x in trades.index.values]
        trades["iteration"] = iteration
        for key in parameter.keys():
            trades[key] = parameter[key]
        return trades