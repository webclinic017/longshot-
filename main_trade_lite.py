from datetime import datetime, timedelta
from processor.processor import Processor as p
from tqdm import tqdm
from alpaca_api.alpaca_api import AlpacaApi
from backtester.backtester_lite import BacktesterLite
from time import sleep
import pandas as pd

alp = AlpacaApi()

closed_orders = alp.live_close_all()
sleep(300)

end = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
start = (datetime.now()-timedelta(days=1) - timedelta(days=10)).strftime("%Y-%m-%d")

sp500 = pd.read_csv("sp500.csv").rename(columns={"Symbol":"ticker"})

parameter = {
'strategy': 'window',
'value': True,
'lookback': 5,
'holding_period': 1,
'ceiling': 10,
'floor': -10
}
iteration = 6
positions = 20
lookback = parameter["lookback"]

simulation = []
for ticker in tqdm(sp500["ticker"]):
    try:
        example = alp.get_ticker_data(ticker,start,end)
        ticker_data = example.df.reset_index().rename(columns={"symbol":"ticker","timestamp":"date","close":"adjclose"})[["date","ticker","adjclose"]]
        ticker_data = p.column_date_processing(ticker_data)
        ticker_data.sort_values("date",inplace=True)
        ticker_data = ticker_data[["date","ticker","adjclose"]]
        ticker_data["prev_close"] = ticker_data["adjclose"]
        ticker_data[f"window_{lookback}"] = ticker_data["prev_close"].shift(lookback)
        ticker_data[f"rolling_{lookback}"] = ticker_data["prev_close"].rolling(lookback).mean()
        simulation.append(ticker_data.dropna())
#         sleep(1)
    except Exception as e:
        print(ticker,str(e))
final = pd.concat(simulation)

final = final[final["date"]==final["date"].max()]

try:
    trades = BacktesterLite.backtest(positions,final.copy(),iteration,parameter,True)
except Exception as e:
    print(str(e))


account = alp.live_get_account()
cash = float(account.cash)

try:
    if trades.index.size > 0: 
        # executing orders
        for row in trades.iterrows():
            try:
                ticker = "BTC/USD" if row[1]["ticker"] == "BTC" else row[1]["ticker"]
                amount = round(cash / positions,2)
                print(ticker,amount)
#                 order_data.append(alp.live_market_order(ticker,amount))
#                 sleep(1)
            except Exception as e:
                print(str(e))
except Exception as e:
        print(str(e))