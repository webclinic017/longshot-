
class Classification(object):

    @classmethod
    def factors(self):
        return ["d1","d2","d3","rolling14"]
    
    @classmethod
    def transform(self,ticker_data):
        ticker_data["d1"] = ticker_data["adjclose"].pct_change(periods=1)
        ticker_data["d2"] = ticker_data["d1"].pct_change(periods=1)
        ticker_data["d3"] = ticker_data["d2"].pct_change(periods=1)
        ticker_data["rolling14"] = ticker_data["adjclose"].rolling(window=14).mean()
        return ticker_data