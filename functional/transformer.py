from processor.processor import Processor as p
from datetime import datetime
class Transformer(object):

    @classmethod
    def financials(self,filing,prices   ,sp500,ticker,included_columns):
        filing["ticker"] = ticker
        filing["date"] = [datetime.strptime(str(x),"%Y%m%d") for x in filing["filed"]]
        filing = p.column_date_processing(filing)
        prices = p.column_date_processing(prices)
        prices = prices.groupby(["year","quarter","ticker"]).mean()
        prices["y"] = prices["adjclose"].shift(-4)
        data = filing.merge(prices,on=["year","quarter","ticker"],how="left")
        data = data.merge(sp500.rename(columns={"Symbol":"ticker"}),on="ticker",how="left")[included_columns]
        return data