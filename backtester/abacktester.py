import pandas as pd
from parameters.parameters import Parameters as params
class ABacktester(object):

    def __init__(self,portfolio_class,current,start_date,end_date):
        self.current = current
        self.start_date = start_date
        self.end_date = end_date
        self.portfolio_class = portfolio_class
        self.table_name = "trades" if self.current else "historical_trades"
    
    ##FINANCIALS DUN WORK NO MORE
    def backtest(self,sim):
        self.portfolio_class.db.connect()
        self.portfolio_class.db.drop(self.table_name)
        backtest_data = sim.copy().dropna()
        parameters = params.parameters()
        for parameter in parameters:
            final_data = backtest_data.copy()
            market_return = parameter["market_return"]
            sell_day = parameter["sell_day"]
            final_data["weekly_return"] = final_data[f"return_{sell_day}"]
            final_data = final_data[final_data["day"]==parameter["buy_day"]]
            final_data = self.portfolio_class.returns.returns(market_return,self.portfolio_class.pricer_class.time_horizon_class,final_data.copy(),False)
            if parameter["rank"] == True:
                final_data = self.portfolio_class.ranker_class.backtest_rank(final_data.copy())
            trades = self.backtest_helper(final_data,parameter,self.start_date,self.end_date)
            self.portfolio_class.db.store(self.table_name,trades)
        self.portfolio_class.db.disconnect()
    
    def recommendation(self,sim,parameter,tyields):
        backtest_data = sim.copy().dropna()
        t = []
        final_data = backtest_data.copy()
        market_return = parameter["market_return"]
        final_data = final_data[final_data["day"]==parameter["buy_day"]-1]
        final_data = self.portfolio_class.returns.returns(market_return,self.portfolio_class.pricer_class.time_horizon_class,final_data.copy(),True,tyields)
        if parameter["rank"] == True:
            final_data = self.portfolio_class.ranker_class.backtest_rank(final_data.copy())
        trades = self.recommendation_helper(final_data,parameter)
        t.append(trades)
        return t
    
    def backtest_helper(self,sim,parameter,start_date,end_date):
        value = parameter["value"]
        ceiling = parameter["ceiling"]
        classification = parameter["classification"]
        short = parameter["short"]
        risk = parameter["risk"]
        # floor = parameter["floor"]
        # floor_value = -0.05
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

        if naming == "week":
            # sim["floor_value_boolean"] = [True in [row[1][f"return_{str(i)}"] <= floor_value for i in range(2,5)] for row in sim.iterrows()]
            # sim["floor_index"] = [min([i if row[1][f"return_{i}"] * row[1]["weekly_delta_sign"] <= floor_value else 5 for i in range(2,5)]) for row in sim.iterrows()]
            # sim["floor_boolean"] = 4 >= sim["floor_index"]
            sim["short_returns"] = [1 + row[1][f"return_{4}"] * row[1]["weekly_delta_sign"] for row in  sim.iterrows()]
            # sim["short_returns"] = [1 + floor_value if row[1]["floor_boolean"] else row[1]["nonfloored_short_returns"] for row in sim.iterrows()]
            sim["returns"] = [1 + row[1][f"{naming}ly_return"] for row in  sim.iterrows()]
        else:   
        ##quarterly logic
            sim["short_returns"] = [1 + row[1][f"{naming}ly_return"] * row[1][f"{naming}ly_delta_sign"] for row in  sim.iterrows()]
            sim["returns"] = [1 + row[1][f"{naming}ly_return"] for row in  sim.iterrows()]
        
        sim["risk_boolean"] = sim[f"{naming}ly_beta"] <= sim[f"{naming}ly_beta"].mean()
        sim["return_boolean"] = sim[f"{naming}ly_delta"] > sim[f"{naming}ly_rrr"]
        ##weekly logic

        test = sim[(sim["return_boolean"]==True)]

        if risk:
            test = test[test["risk_boolean"] ==True]    
        if classification and "classification_prediction" in test.columns:
            test = test[test["classification_prediction"]==1.0]
                
        if ceiling:
            test = test[test[f"{naming}ly_delta"]<=1]

        ledgers = []
        ## ledger creation
        stuff = ["year",naming,"ticker",f"{naming}ly_delta",f"{naming}ly_delta_sign","short_returns","returns"]
        for i in range(positions):    
            ledger = test.sort_values(["year",naming,f"{naming}ly_delta"])[stuff].groupby(["year",naming],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()

        final["actual_returns"] = final[return_column]

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        return final
    
    def recommendation_helper(self,sim,parameter):
        value = parameter["value"]
        ceiling = parameter["ceiling"]
        classification = parameter["classification"]
        short = parameter["short"]
        naming = self.portfolio_class.pricer_class.time_horizon_class.naming_convention
        positions = self.portfolio_class.pricer_class.positions
        ## optimizing
        
        if value:
            sim[f"{naming}ly_delta_sign"] = sim[f"{naming}ly_delta_sign"] * -1
            if "classification_prediction" in sim.columns:
                sim["classification_prediction"] = [int(not x) for x in sim["classification_prediction"]]

        if not short:
            sim = sim[sim[f"{naming}ly_delta_sign"] == 1]
        
        sim["risk_boolean"] = sim[f"{naming}ly_beta"] <= sim[f"{naming}ly_beta"].mean()
        sim["return_boolean"] = sim[f"{naming}ly_delta"] > sim[f"{naming}ly_rrr"]
        ##weekly logic

        test = sim[(sim["return_boolean"]==True) & (sim["risk_boolean"]==True)]
        
        if classification and "classification_prediction" in test.columns:
            test = test[test["classification_prediction"]==1.0]
                
        if ceiling:
            test = test[test[f"{naming}ly_delta"]<=1]

        ledgers = []
        ## ledger creation
        stuff = ["year",naming,"ticker",f"{naming}ly_delta",f"{naming}ly_delta_sign"]
        for i in range(positions):    
            ledger = test.sort_values(["year",naming,f"{naming}ly_delta"])[stuff].groupby(["year",naming],sort=False).nth(-i-1)
            ledger["position"] = i
            ledgers.append(ledger)
        final = pd.concat(ledgers).reset_index()

        ## labeling
        for key in parameter.keys():
            final[key] = parameter[key]

        ## storing
        return final