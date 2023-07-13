import pandas as pd
class ABacktester(object):

    def __init__(self,portfolio_class,current,start_date,end_date):
        self.current = current
        self.start_date = start_date
        self.end_date = end_date
        self.portfolio_class = portfolio_class
        self.table_name = "trades" if self.current else "historical_trades"
    
    def backtest(self,sim,tyields,parameter,rec):
        backtest_data = sim.copy().dropna()
        final_data = backtest_data.copy()
        market_return = parameter["market_return"]
        sell_day = parameter["sell_day"]
        mod_val = int(sell_day / 5)
        day_offset = 1 if rec else 0
        if parameter["rank"] == True:
            final_data = self.portfolio_class.ranker_class.backtest_rank(final_data.copy())
        if not rec:
            final_data = final_data[final_data["week"] % mod_val == 0]
            final_data["weekly_return"] = final_data[f"return_{sell_day}"]
        final_data = final_data[final_data["day"]==parameter["buy_day"]-day_offset]
        final_data = self.portfolio_class.returns.returns(market_return,self.portfolio_class.pricer_class.time_horizon_class,final_data.copy(),rec,tyields)
        trades = self.backtest_helper(final_data,parameter,self.start_date,self.end_date,rec)
        return trades
        
    def backtest_return_helper(self,sim,naming):
        if naming == "week":
            # sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
            # sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["weekly_delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
            # sim["floor_boolean"] = 4 >= sim["floor_index"]
            # sim["short_returns"] = [1 + row[1][f"return_{4}"] * row[1]["weekly_delta_sign"] for row in  sim.iterrows()]
            # sim["short_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["nonfloored_short_returns"] for row in sim.iterrows()]
            sim["returns"] = [1 + row[1][f"{naming}ly_return"] for row in  sim.iterrows()]
        else:   
        ##quarterly logic
            sim["short_returns"] = [1 + row[1][f"{naming}ly_return"] * row[1][f"{naming}ly_delta_sign"] for row in  sim.iterrows()]
            sim["returns"] = [1 + row[1][f"{naming}ly_return"] for row in  sim.iterrows()]
        return sim
    
    def backtest_helper(self,sim,parameter,start_date,end_date,current):
        value = parameter["value"]
        ceiling = parameter["ceiling"]
        classification = parameter["classification"]
        short = parameter["short"]
        risk = parameter["risk"]
        floor_value = parameter["floor_value"]
        naming = self.portfolio_class.pricer_class.time_horizon_class.naming_convention
        positions = self.portfolio_class.pricer_class.positions
        sim = sim[(sim["year"] >= start_date.year) & (sim["year"] <= end_date.year)]

        ## optimizing
        if value:
            sim[f"{naming}ly_delta_sign"] = sim[f"{naming}ly_delta_sign"] * -1
            if "classification_prediction" in sim.columns:
                sim["classification_prediction"] = [int(not x) for x in sim["classification_prediction"]]

        if not short:
            sim = sim[sim[f"{naming}ly_delta_sign"] == 1.0]
            return_column = "returns"
        else:
            return_column = "short_returns"
        
        columns = ["year",naming,"ticker",f"{naming}ly_delta",f"{naming}ly_delta_sign"]
        if not current:
            sim = self.backtest_return_helper(sim,naming)
            sim["actual_returns"] = sim[return_column]
            columns.append("actual_returns")

        sim["risk_boolean"] = sim[f"{naming}ly_beta"] <= sim[f"{naming}ly_beta"].mean()
        sim["return_boolean"] = sim[f"{naming}ly_delta"] > sim[f"{naming}ly_rrr"]
        
        ##weekly logic
        test = sim.copy()
        if risk == "rrr":
            test = sim[(sim["return_boolean"]==True)]
            test = test[test["risk_boolean"] ==True]
        elif risk == "flat":
            test = sim[(sim[f"{naming}ly_delta"]>=0.05)]
        else:
            test = test.copy()

        if classification and "classification_prediction" in test.columns:
            test = test[test["classification_prediction"]==1.0]
                
        if ceiling:
            test = test[test[f"{naming}ly_delta"]<=floor_value]

        ledgers = []

        ## ledger creation
        for i in range(positions):    
            ledger = test.sort_values(["year",naming,f"{naming}ly_delta"])[columns].groupby(["year",naming],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        return final