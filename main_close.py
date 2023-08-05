from time import sleep
from alpaca_api.alpaca_api import AlpacaApi

alp = AlpacaApi()
closed_orders = alp.live_close_all()
sleep(300)