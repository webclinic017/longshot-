import pandas as pd
from datetime import datetime

class Visualizer(object):

    def __init__(self,portfolio):
        self.portfolio = portfolio


    def visualize(self,prices):
        initial = 100000
        max_date = self.portfolio.pull_visualization_max_date()
        positions = self.portfolio.positions
        parameter = self.portfolio.pull_parameters()
        self.portfolio.db.connect()
        if parameter.index.size > 0:
            for today in pd.bdate_range(max_date,datetime.now()):
                todays_prices = prices[prices["date"]==today]
                self.portfolio.initialize_portfolio(parameter,initial,today)
                if todays_prices.index.size > 0:
                    portfolio = self.portfolio.portfolio_state
                    position_states = []
                    for position in portfolio["positions"].keys():
                        current_date = portfolio["date"]
                        position_dictionary = portfolio["positions"][position]
                        state = {"date":current_date,"position":position,"cash":position_dictionary["cash"]}
                        asset = position_dictionary["asset"]
                        if  asset != {}:
                            state["ticker"] = asset["ticker"]
                            state["amount"] = asset["amount"]
                        else:
                            state["ticker"] = "LOL"
                            state["amount"] = 0
                        position_states.append(state)
                    final = pd.DataFrame(position_states)
                    ds = final.merge(todays_prices,on=["date","ticker"],how="left").fillna(0)
                    ds["asset_pv"] = ds["amount"] * ds["adjclose"]
                    ds["pv"] = ds["cash"] + ds["asset_pv"]
                    visualization = ds.pivot_table(index="date",columns="position",values="pv").reset_index()
                    final = visualization.reset_index(drop=True)
                    for i in range(positions):
                        final.rename(columns={i:str(i)},inplace=True)
                    self.portfolio.db.store(f"visualization",final)
        self.portfolio.db.disconnect()