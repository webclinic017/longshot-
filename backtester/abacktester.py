import pandas as pd
class ABacktester(object):

    def __init__(self,trade_algorithm,current,start_date,end_date):
        self.current = current
        self.start_date = start_date
        self.end_date = end_date
        self.trade_algorithm = trade_algorithm
        self.table_name = "trades" if self.current else "historical_trades"
    
    def backtest(self,sim,parameter,rec):
        final_data = sim.dropna()
        sell_day = self.trade_algorithm.pricer_class.time_horizon_class.holding_period
        mod_val = int(sell_day / 5)
        buy_day = parameter["buy_day"]
        if parameter["rank"] == True:
            final_data = self.trade_algorithm.ranker_class.backtest_rank(final_data)
        if not rec:
            naming = self.trade_algorithm.pricer_class.time_horizon_class.naming_convention
            if naming != "date":
                final_data = final_data[final_data["week"] % mod_val == 0]
                final_data = final_data[final_data["day"]==buy_day]
        trades = self.backtest_helper(final_data,parameter,self.start_date,self.end_date,rec)
        return trades
        
    def backtest_return_helper(self,sim,naming):
        sim["returns"] = sim["return"] + 1
        return sim
    
    def backtest_helper(self,sim,parameter,start_date,end_date,current):
        value = parameter["value"]
        ceiling = parameter["ceiling"]
        classification = parameter["classification"]
        tyields = parameter["tyields"]
        risk = parameter["risk"]
        floor_value = parameter["floor_value"]
        naming = self.trade_algorithm.pricer_class.time_horizon_class.naming_convention
        positions = self.trade_algorithm.positions
        sim = sim[(sim["year"] >= start_date.year) & (sim["year"] <= end_date.year)]

        ## optimizing
        if not value:
            sim[f"{naming}ly_delta_sign"] = sim[f"{naming}ly_delta_sign"] * -1
            if "classification_prediction" in sim.columns:
                sim["classification_prediction"] = [int(not x) for x in sim["classification_prediction"]]

        sim = sim[sim[f"{naming}ly_delta_sign"]==1]
        return_column = "returns"
        columns = ["year",naming,"ticker",f"{naming}ly_delta",f"{naming}ly_delta_sign"]
        if not current:
            sim = self.backtest_return_helper(sim,naming)
            sim["actual_returns"] = sim[return_column]
            columns.append("actual_returns")
        
        ##weekly logic
        test = sim.copy()
        ## Filtering
        if risk == "rrr":
            test["risk_boolean"] = test[f"{naming}ly_beta"] <= test[f"{naming}ly_beta"].mean()
            test["return_boolean"] = test[f"{naming}ly_delta"] > test[f"{naming}ly_rrr_{tyields}"]
            test = test[(test["return_boolean"]==True)]
            test = test[test["risk_boolean"] ==True]
        elif risk == "flat":
            test = test[(test[f"{naming}ly_delta"]>=0.05)]
        else:
            test = test.copy()
            
        if classification and "classification_prediction" in test.columns:
            test = test[test["classification_prediction"]==1.0]
        if ceiling:
            test = test[test[f"{naming}ly_delta"]<=floor_value]

        ledgers = []
        if current:
            test = test[test["date"]==test["date"].max()]
        ## ledger creation
        for i in range(positions):    
            ledger = test.sort_values(["year",naming,f"{naming}ly_delta"])[columns].groupby(["year",naming],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()
        final["iteration"] = parameter["iteration"]

        ## storing
        return final