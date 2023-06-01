
class Financial(object):

    @classmethod
    def transform(self,prices,filing):
        filing = filing.groupby(["year","quarter"]).mean().reset_index()
        filing["year"] = filing["year"] + 1
        if "commonstockdividendspersharecashpaid" in filing.columns:
            filing["commonstockdividendspersharecashpaid"] = filing["commonstockdividendspersharecashpaid"].fillna(0)
        else:
            filing["commonstockdividendspersharecashpaid"] = 0
        ticker_data = prices.groupby(["year","quarter","ticker"]).mean().reset_index()
        ticker_data["adjclose"] = [float(x) for x in ticker_data["adjclose"]]
        ticker_data = ticker_data.merge(filing,on=["year","quarter"],how="left").reset_index()
        return ticker_data