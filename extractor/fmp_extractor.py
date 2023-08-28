from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("FMPKEY")

## api middle man to handle api requests with tiingo
class FMPExtractor(object):

    @classmethod
    def extract(self,ticker,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "apikey":token,
                "limit":120,
            }
            url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
            requestResponse = requests.get(url,headers=headers,params=params)
            return pd.DataFrame(requestResponse.json())
        except Exception as e:
            print(str(e))