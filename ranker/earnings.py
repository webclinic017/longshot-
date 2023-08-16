from ranker.nonairanker import NonAIRanker
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class Earnings(NonAIRanker):

    def __init__(self):
        super().__init__(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
        self.naming_suffix = "earnings_rank"
        self.lower_bound = 0
        
    def training_set_helper(self,ticker,prices,filing,current):
        filing = filing.groupby(["year","quarter"]).mean().reset_index()
        ticker_data = prices.copy()
        ticker_data.sort_values("date",ascending=True,inplace=True)
        ticker_data["year"] = [row[1]["year"] if row[1]["quarter"] < 4 else row[1]["year"] + 1 for row in ticker_data.iterrows()]
        ticker_data["quarter"] = [row[1]["quarter"] + 1 if row[1]["quarter"] < 4 else 1 for row in ticker_data.iterrows()]
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.groupby(["year","quarter"]).mean().reset_index()
        ticker_data["ticker"] = ticker
        ticker_data = ticker_data.merge(filing,on=["year","quarter"],how="left").reset_index()
        ticker_data["rank"] = ticker_data["adjclose"] / ticker_data["earningspersharebasic"]
        if not current:
            ticker_data["y"] = ticker_data["earningspersharebasic"].shift(-4)
            columns = self.all_columns
        else:
            columns = self.all_columns[:-1]
        ticker_data = ticker_data[columns].dropna()
        return ticker_data