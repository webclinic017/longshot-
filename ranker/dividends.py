from data_product.afinancialstatement_data_product import AFinancialStatementDataProduct
from time_horizons.time_horizons import TimeHorizons
from asset_classes.asset_classes import AssetClasses

class Dividends(AFinancialStatementDataProduct):

    def __init__(self):
        super().__init__(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
        self.naming_suffix = "dividends_rank"
        self.lower_bound = 0
        
    def training_set_helper(self,ticker_data,filing,current):
        filing = filing.groupby(["year","quarter"]).mean().reset_index()
        filing["year"] = [row[1]["year"] if row[1]["quarter"] != 4 else row[1]["year"]+1 for row in filing.iterrows()]
        filing["quarter"] = [row[1]["quarter"]+1 if row[1]["quarter"] != 4 else 1 for row in filing.iterrows()]
        if "dividendscommonstockcash" not in filing.columns:
            filing["dividendscommonstockcash"] = 0
        if "weightedaveragenumberofsharesoutstandingbasic" not in filing.columns:
            filing["weightedaveragenumberofsharesoutstandingbasic"] = 0
        if "earningspersharebasic" not in filing.columns:
            filing["earningspersharebasic"] = 0
        filing["dividend"] = filing["dividendscommonstockcash"] / filing["weightedaveragenumberofsharesoutstandingbasic"]
        ticker_data = ticker_data.merge(filing[["year","quarter","dividend"]],on=["year","quarter"],how="left").reset_index()
        ticker_data["rank"] = ticker_data["dividend"] / ticker_data["adjclose"] 
        return ticker_data