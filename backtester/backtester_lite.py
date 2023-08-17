
class BacktesterLite(object):
    
    @classmethod
    def backtest(self,positions,final,iteration,parameter,current):
        iteration_sim = final.copy()
        industry_weighted = parameter["industry_weighted"]
        strategy = parameter["strategy"]
        lookback = parameter["lookback"]
        holding_period = parameter["holding_period"]
        floor = parameter["floor"]
        ceiling = parameter["ceiling"]
        volatility = parameter["volatility"]
        local_min = parameter["local_min"]
        iteration_sim["signal"] = (iteration_sim[f"{strategy}_{lookback}"] - iteration_sim["prev_close"]) / iteration_sim["prev_close"]
        iteration_sim = iteration_sim[iteration_sim[f"rolling_pct_stdev_{lookback}"]<=volatility]
        iteration_sim = iteration_sim[(iteration_sim["signal"]>=floor) & (iteration_sim["signal"]<=ceiling)]

        if not parameter["value"]:
            iteration_sim["signal"] = iteration_sim["signal"] * -1
            
        if local_min:
            iteration_sim = iteration_sim[(iteration_sim["d1"]==0.0)]

        additional_columns = ["signal"]
        
        if not current:
            additional_columns.append("return")
            iteration_sim["return"] = iteration_sim[f"return_{holding_period}"]
            if holding_period != 1:
                mod_val = int(holding_period / 5)
                iteration_sim = iteration_sim[iteration_sim["week"] % mod_val == 0]
                iteration_sim = iteration_sim[iteration_sim["day"]==1]

        if industry_weighted:
            iteration_sim = iteration_sim.sort_values(["date","signal"],ascending=False).groupby(["date","GICS Sector"]).first().reset_index()   

        trades = iteration_sim.sort_values(["date","signal"]).groupby("date").nth([-1-i for i in range(positions)]).reset_index()[["date","ticker"]+additional_columns]
        trades["position"] = [int(x % positions) for x in trades.index.values]
        trades["iteration"] = iteration
        for key in parameter.keys():
            trades[key] = parameter[key]
        return trades