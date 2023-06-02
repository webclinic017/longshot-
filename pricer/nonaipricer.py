from processor.processor import Processor as p
import pandas as pd
from database.adatabase import ADatabase
import numpy as np
from strategy.astrategy import AStrategy

class NonAIStrategy(AStrategy):

    def __init__(self,asset_class,quarterly):
        super().__init__(asset_class,quarterly)
        self.isai = False

    
    def create_sim(self,simulation,price_returns):
        sim = price_returns.merge(self.tyields[["year",self.group_timeframe,f"{self.group_timeframe}ly_yield"]],on=["year",self.group_timeframe],how="left")
        colcol = [x for x in simulation.columns if self.strat_class.name in x] + ["year",self.group_timeframe,"ticker"]
        sim = sim.merge(simulation[colcol],on=["year",self.group_timeframe,"ticker"],how="left")
        sim = sim.dropna().groupby(["year",self.group_timeframe,"date","ticker"]).mean().reset_index()
        return sim