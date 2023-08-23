from processor.processor import Processor as p
import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

class Transformer(object):

    @classmethod
    def transform(self,ticker_data,lookbacks,current):
        ticker_data = p.column_date_processing(ticker_data)[["date","ticker","adjopen"]]
        ticker_data.sort_values("date",inplace=True)
        ticker_data["day"] = [x.weekday() for x in ticker_data["date"]]
        ticker_data["prev_open"] = ticker_data["adjopen"].shift(1)
        price_col = "adjopen" if current else "prev_open"
        for lookback in lookbacks:
            ticker_data[f"window_{lookback}"] = ticker_data[price_col].shift(lookback)
            ticker_data[f"rolling_{lookback}"] = ticker_data[price_col].rolling(lookback).mean()
            # ticker_data["d1"] = ticker_data[f"adjopen"].pct_change()
            # ticker_data[f"rolling_stdev_{lookback}"] = ticker_data[price_col].rolling(lookback).std()
            # ticker_data[f"rolling_pct_stdev_{lookback}"] = ticker_data[f"rolling_stdev_{lookback}"] / ticker_data[f"rolling_{lookback}"]
        if not current:
            ticker_data[f"return"] = (ticker_data["adjopen"].shift(-1) - ticker_data["adjopen"]) / ticker_data["adjopen"]
        return ticker_data