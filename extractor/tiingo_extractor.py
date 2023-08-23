from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("TIINGO")

## api middle man to handle api requests with tiingo
class TiingoExtractor(object):

    @classmethod
    def extract(self,ticker,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "token":token,
                "startDate":start,
                "endDate":end
            }
            url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
            requestResponse = requests.get(url,headers=headers,params=params)
            return pd.DataFrame(requestResponse.json())
        except Exception as e:
            print(str(e))
    
    @classmethod
    def crypto(self,crypto,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }
            params = {
                "token":token
            }
            url = f"https://api.tiingo.com/tiingo/crypto/prices?tickers={crypto}usd,fldcbtc&startDate{start}&endDate={end}&resampleFreq=1Day"
            requestResponse = requests.get(url,headers=headers,params=params)
            return pd.DataFrame(requestResponse.json()[0]["priceData"]).rename(columns={"close":"adjopen"})
        except Exception as e:
            print(str(e))

