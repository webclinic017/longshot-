import pandas as pd

## data standardizer
class Processor(object):
    
    ## standardizes timeseries data and column formats
    @classmethod
    def column_date_processing(self,data):
        new_cols = {}
        for column in data.columns:
            new_cols[column] = column.lower().replace(" ","")
        for col in new_cols:
            data.rename(columns={col:new_cols[col]},inplace=True)
        if "date" in list(data.columns):
            try:
                data["date"] = pd.to_datetime(data["Date"]).dt.tz_localize(None)
            except:
                data["date"] = pd.to_datetime(data["date"]).dt.tz_localize(None)
            data["year"] = data["date"].dt.year
            data["quarter"] = data["date"].dt.quarter
            data["week"] = data["date"].dt.isocalendar().week
        return data