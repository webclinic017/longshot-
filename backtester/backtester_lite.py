
class BacktesterLite(object):
    
    @classmethod
    def backtest(self,positions,final,iteration,parameter,current):
        iteration_sim = final.copy()
        strategy = parameter["strategy"]
        lookback = parameter["lookback"]
        ceiling = parameter["ceiling"]
        floor = parameter["floor"]
        holding_period = parameter["holding_period"]
        iteration_sim["signal"] = (iteration_sim[f"{strategy}_{lookback}"] - iteration_sim["prev_close"]) / iteration_sim["prev_close"] 
        if not parameter["value"]:
            iteration_sim["signal"] = iteration_sim["signal"] * -1
        if not current:
            iteration_sim["return"] = iteration_sim[f"return_{holding_period}"]
            additional_column = "return"
        else:
            additional_column = "signal"
        iteration_sim = iteration_sim[(iteration_sim["signal"]>=floor) & (iteration_sim["signal"]<=ceiling)]
        trades = iteration_sim.sort_values(["date","signal"]).groupby("date",as_index=True).nth([-1-i for i in range(positions)]).reset_index()[["date","ticker",additional_column]]
        trades["position"] = [int(x % positions) for x in trades.index.values]
        trades["iteration"] = iteration
        for key in parameter.keys():
            trades[key] = parameter[key]
        return trades