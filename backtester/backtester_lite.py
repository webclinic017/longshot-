
class BacktesterLite(object):
    
    @classmethod
    def backtest(self,positions,final,iteration,parameter,current):
        iteration_sim = final.copy()
        strategy = parameter["strategy"]
        lookback = parameter["lookback"]
        holding_period = parameter["holding_period"]
        floor = parameter["floor"]
        ceiling = parameter["ceiling"]
        iteration_sim["signal"] = (iteration_sim[f"{strategy}_{lookback}"] - iteration_sim["prev_close"]) / iteration_sim["prev_close"]
        iteration_sim = iteration_sim[(iteration_sim["signal"]>=floor) & (iteration_sim["signal"]<=ceiling)]

        if not parameter["value"]:
            iteration_sim["signal"] = iteration_sim["signal"] * -1
        
        additional_columns = ["signal"]
        
        if not current:
            additional_columns.append("return")
            iteration_sim["return"] = iteration_sim[f"return_{holding_period}"]
            if holding_period != 1:
                mod_val = int(holding_period / 5)
                iteration_sim = iteration_sim[iteration_sim["week"] % mod_val == 0]
                iteration_sim = iteration_sim[iteration_sim["day"]==1]
            
        trades = iteration_sim.sort_values(["date","signal"]).groupby("date",as_index=True).nth([-1-i for i in range(positions)]).reset_index()[["date","ticker"]+additional_columns]
        trades["position"] = [int(x % positions) for x in trades.index.values]
        trades["iteration"] = iteration
        for key in parameter.keys():
            trades[key] = parameter[key]
        return trades