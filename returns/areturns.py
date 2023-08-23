from processor.processor import Processor as p

class AReturns(object):

    def __init__(self,name):
        self.name = name
    
    def returns(self,time_horizon_class,ticker_sim,current):
        ticker_sim = p.column_date_processing(ticker_sim)
        ticker_sim = ticker_sim.sort_values("date")
        ticker_sim["prev_open"] = ticker_sim["adjopen"].shift(1)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        holding_period = time_horizon_class.holding_period
        if not current:
            ticker_sim["return"] = (ticker_sim["adjopen"].shift(-holding_period) - ticker_sim["adjopen"]) / ticker_sim["adjopen"]
        ticker_sim["risk_return"] = (ticker_sim["adjopen"].shift(1) - ticker_sim["adjopen"].shift(1+holding_period)) / ticker_sim["adjopen"].shift(1+holding_period)
        return ticker_sim