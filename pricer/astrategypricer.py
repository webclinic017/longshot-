from processor.processor import Processor as p
from pricer.apricer import APricer

class AStrategyPricer(APricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.factors = [str(x) for x in range(14)]
        self.included_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.included_live_columns = ["year",self.time_horizon_class.naming_convention,"ticker","adjclose","y"]
        self.all_columns = self.factors + self.included_columns
        
    def price_returns(self,ticker_sim,current):
        ticker_sim = p.column_date_processing(ticker_sim)
        ticker_sim = ticker_sim.sort_values("date")
        ticker_sim["prev_close"] = ticker_sim["adjclose"].shift(1)
        ticker_sim["day"] = [x.weekday() for x in ticker_sim["date"]]
        holding_period = self.time_horizon_class.holding_period
        if not current:
            ticker_sim[f"return"] = (ticker_sim["adjclose"].shift(-holding_period) - ticker_sim["adjclose"]) / ticker_sim["adjclose"]
        ticker_sim[f"{self.time_horizon_class.naming_convention}ly_risk_return"] = (ticker_sim["adjclose"].shift(1) - ticker_sim["adjclose"].shift(1+holding_period)) / ticker_sim["adjclose"].shift(1+holding_period)
        return ticker_sim