from alpaca.trading.client import TradingClient
from dotenv import load_dotenv
import os
load_dotenv()
api_key = os.getenv("ALPACAKEYID")
secret_key = os.getenv("ALPACASECRETKEY")
live_api_key = os.getenv("ALPACALIVEKEYID")
live_secret_key = os.getenv("ALPACALIVESECRETKEY")
# from alpaca.trading.requests import GetAssetsRequest
# from alpaca.trading.enums import AssetClass
# from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest


## Description: alpaca trading api interface
class AlpacaApi(object):

    def __init__(self):
        self.paper_trading_client = TradingClient(api_key, secret_key, paper=True)
        self.live_trading_client = TradingClient(live_api_key, live_secret_key, paper=False)
    
    ## retrieve the paper account
    def paper_get_account(self):
        return self.paper_trading_client.get_account()
    
    def paper_close_all(self):
        return self.paper_trading_client.close_all_positions(cancel_orders=False)
    
    def paper_market_order(self,ticker,amount):
        market_order_data = MarketOrderRequest(
                    symbol=ticker,
                    notional=amount,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
        market_order = self.paper_trading_client.submit_order(
                order_data=market_order_data
               )
        return market_order
    
    def paper_close_btc(self):
        return self.paper_trading_client.close_position("BTC/USD")
    
    ## retrieve the live account
    def live_get_account(self):
        return self.live_trading_client.get_account()
    
    def live_close_all(self):
        return self.live_trading_client.close_all_positions(cancel_orders=False)
    
    def live_market_order(self,ticker,amount):
        market_order_data = MarketOrderRequest(
                    symbol=ticker,
                    notional=amount,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
        market_order = self.live_trading_client.submit_order(
                order_data=market_order_data
               )
        return market_order