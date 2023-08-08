from database.adatabase import ADatabase
import pandas as pd
from datetime import datetime, timedelta

## description: Market database class for market based functions
class Market(ADatabase):
    
    def __init__(self):
        super().__init__("market")

    def retrieve_tickers(self,currency):
        try:
            db = self.client[self.name]
            table = db[currency]
            data = table.find({},{"ticker":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))    

    def retrieve_ticker_prices(self,currency,ticker):
        try:
            db = self.client[self.name]
            table = db[currency]
            data = table.find({"ticker":ticker},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))

    def retrieve_eps_filings(self,industry):
        try:
            db = self.client[self.name]
            table = db["eps_filings"]
            data = table.find({"industry":industry},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))  
              
    def retrieve_cfa_filings(self,ticker):
        try:
            db = self.client[self.name]
            table = db["cfa_filings"]
            data = table.find({"ticker":ticker},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
    
    def retrieve_high_level_filings(self,ticker):
        try:
            db = self.client[self.name]
            table = db["high_level_filings"]
            data = table.find({"ticker":ticker},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
            
    def retrieve_max_date(self,database_name):
        try:
            data = self.retrieve_collection_date_range(database_name)
            if data.index.size < 1:
                max_date = datetime(2012,1,1).strftime("%Y-%m-%d")
            else:
                max_date = (datetime.strptime(data.iloc[-1].item().split("T")[0],"%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            return max_date
        except Exception as e:
            print(str(e))