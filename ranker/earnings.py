from data_product.afinancialstatement_data_product import AFinancialStatementDataProduct
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class Earnings(AFinancialStatementDataProduct):

    def __init__(self):
        super().__init__(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
        self.naming_suffix = "earnings_rank"
        self.lower_bound = 0
        
    def training_set_helper(self,ticker_data,filing,current):
        filing = filing.groupby(["year","quarter"]).mean().reset_index()
        filing["year"] = [row[1]["year"] if row[1]["quarter"] != 4 else row[1]["year"]+1 for row in filing.iterrows()]
        filing["quarter"] = [row[1]["quarter"]+1 if row[1]["quarter"] != 4 else 1 for row in filing.iterrows()]
        ticker_data = ticker_data.merge(filing[["year","quarter","earningspersharebasic"]],on=["year","quarter"],how="left").reset_index()
        ticker_data["rank"] = ticker_data["adjclose"] / ticker_data["earningspersharebasic"]
        return ticker_data