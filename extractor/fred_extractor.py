import requests
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("FREDKEY")
import pandas as pd
import numpy as np

class FREDExtractor(object):

    @classmethod
    def tyields(self,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "api_key":token,
                "observation_start":start,
                "observation_end":end
            }

            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS1&file_type=json"
            requestResponse = requests.get(url,headers=headers,params=params).json()
            stuff = pd.DataFrame(requestResponse["observations"])
            stuff["value"] = [float(x) if x != "." else np.NAN for x in stuff["value"]]
            return stuff.dropna()
        except Exception as e:
            print(str(e))

    @classmethod
    def tyields10(self,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "api_key":token,
                "observation_start":start,
                "observation_end":end
            }

            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&file_type=json"
            requestResponse = requests.get(url,headers=headers,params=params).json()
            stuff = pd.DataFrame(requestResponse["observations"])
            stuff["value"] = [float(x) if x != "." else np.NAN for x in stuff["value"]]
            return stuff.dropna()
        except Exception as e:
            print(str(e))
    
    @classmethod
    def spy(self,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "api_key":token,
                "observation_start":start,
                "observation_end":end,
                "frequency":"w"
            }

            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=SP500&file_type=json"
            requestResponse = requests.get(url,headers=headers,params=params).json()
            stuff = pd.DataFrame(requestResponse["observations"])
            stuff["value"] = [float(x) if x != "." else np.NAN for x in stuff["value"]]
            return stuff.dropna()
        except Exception as e:
            print(str(e))