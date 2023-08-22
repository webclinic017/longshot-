import pandas as pd
from alpaca_api.alpaca_api import AlpacaApi
from time import sleep

alp = AlpacaApi()
try:
    print("closing")
    alp.live_close_all()
    sleep(300)
except Exception as e:
    print("close",str(e))

account = alp.live_get_account()
cash = float(account.cash) * 0.999
trades = pd.read_csv("trades.csv")
positions = trades.index.size

for row in trades.iterrows():
    try:
        ticker = row[1]["ticker"]
        amount = float(cash/positions)
        alp.live_market_order(ticker,amount)
        print(ticker,amount)
    except Exception as e:
        print(ticker,amount,str(e))