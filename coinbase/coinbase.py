
import pandas as pd
import requests
from datetime import date, timedelta
from coinbase.coinbase_wallet_auth import CoinbaseWalletAuth

class Coinbase(object):
    def __init__(self,version,api_key,api_secret,api_passphrase):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        if version == "test":
            self.base_url = "https://api-public.sandbox.exchange.coinbase.com"
        else:
            self.base_url = "https://api.exchange.coinbase.com"
            
    def get_current_price(self,crypto):
        auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
        product_id = f'{crypto}-USD'
        url = f"{self.base_url}/products/{product_id}/ticker"
        r = requests.get(url, auth=auth)
        return r.json()
    
    def get_timeframe_prices(self,crypto,start,end,timeframe):
        try:
            auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
            product_id = f'{crypto}-USD'
            start = end - timedelta(days=timeframe)
            url = f"{self.base_url}/products/{product_id}/candles"            
            params = {"granularity":86400,
                     "start":start.strftime("%Y-%m-%d"),
                     "end":end.strftime("%Y-%m-%d")}
            r = requests.get(url, auth=auth,params=params)
            if len(r.json()) > 0:
                results = pd.DataFrame(r.json(),columns=["timestamp", "low", "high", "open", "close","volume"])
                results["date"] = [str(date.fromtimestamp(x)) for x in results["timestamp"]]
                results["crypto"] = crypto
                return results
            else:
                return pd.DataFrame([{}])
        except Exception as e:
            print(product_id,start,end,str(e))
            return product_id,start,end,str(e)

    def get_prices(self,crypto,start,end):
        try:
            auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
            product_id = f'{crypto}-USD'
            url = f"{self.base_url}/products/{product_id}/candles"            
            params = {"granularity":86400,
                     "start":start.strftime("%Y-%m-%d"),
                     "end":end.strftime("%Y-%m-%d")}
            r = requests.get(url, auth=auth,params=params)
            if len(r.json()) > 0:
                results = pd.DataFrame(r.json(),columns=["timestamp", "low", "high", "open", "close","volume"])
                results["date"] = [str(date.fromtimestamp(x)) for x in results["timestamp"]]
                results["crypto"] = crypto
                return results
            else:
                return pd.DataFrame([{}])
        except Exception as e:
            print(product_id,start,end,str(e))
            return product_id,start,end,str(e)
    
    def get_accounts(self):
        try:
            auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
            api_url = f"{self.base_url}/accounts"
            r = requests.get(api_url, auth=auth)
            results = pd.DataFrame(r.json())
            results["balance"] = results["balance"].astype(float)
            return results
        except Exception as e:
            print(str(e))
            return str(e)

    
    def get_orders(self):
        try:
            auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
            url = f"{self.base_url}/orders"
            params = {"sorting":"desc"
                    ,"status":"open"
                    ,"sortedBy":"created_at"
                    ,"limit":100}
            r = requests.get(url, auth=auth, params=params)
            results = pd.DataFrame(r.json())
            return results
        except Exception as e:
            print(str(e))
            return str(e)
    
    def get_fill(self,crypto):
        try:
            auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
            url = f"{self.base_url}/fills"
            product_id = f'{crypto}-USD'
            params = {
                    "product_id":product_id,
                    "limit":100}
            r = requests.get(url, auth=auth, params=params)
            return r.json()
        except Exception as e:
            print(str(e))
            return str(e)
    def create_fill_report(self,start,end):
        payload = {
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "type": "fills",
        "format": "csv",
        "product_id": "ALL"
        }
        url =f"{self.base_url}/reports"
        auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
        r = requests.post(url, auth=auth, json=payload)
        return r.json()
        
    def get_fill_report(self):
        url =f"{self.base_url}/reports"
        auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
        params = {
                    "type":"fills",
                    "limit":100
                }
        r = requests.get(url, auth=auth, params=params)
        return r.json()
    
    def place_buy(self,crypto,buy_price,size):
        auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
        url = f"{self.base_url}/orders"
        product_id = f'{crypto}-USD'
        payload = {
            "product_id": product_id,
            "type": "limit",
            "side": "buy",
            "stp": "dc",
            "time_in_force": "GTT",
            "cancel_after": "day",
            "post_only": "false",
            "price":round(buy_price,2),
            "size":size
        }
        response = requests.post(url,auth=auth,json=payload)
        return response.json()
    
    def cancel_order(self,order_id):
        auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
        url = f"{self.base_url}/orders"
        params = {
            "order_id":order_id
        }
        response = requests.delete(url,auth=auth,params=params)
        return response

    def place_sell(self,product_id,sell_price,size):
        auth = CoinbaseWalletAuth(self.api_key,self.api_secret,self.api_passphrase)
        url = f"{self.base_url}/orders"
        payload = {
            "product_id": product_id,
            "type": "limit",
            "side": "sell",
            "stp": "dc",
            "time_in_force": "GTC",
            "post_only": "false",
            "price":round(sell_price,2),
            "size":size
        }
        response = requests.post(url,auth=auth,json=payload)
        return response.json()