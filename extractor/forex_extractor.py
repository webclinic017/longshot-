from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("EXCHANGERATES")

class FOREXExtractor(object):

    @classmethod
    def extract(self,start,end):
        try:
            headers = {
                "Content-Type":"application/json",
                "apikey":token,
            }

            params = {
                "base":"USD",
                "start_date":start,
                "end_date":end
            }
            url = f"https://api.apilayer.com/exchangerates_data/timeseries?"
            requestResponse = requests.get(url,headers=headers,params=params)
            data = pd.DataFrame(requestResponse.json())
            return data
        except Exception as e:
            print(str(e))
    
