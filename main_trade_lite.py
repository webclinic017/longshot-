from datetime import datetime, timedelta
from processor.processor import Processor as p
from alpaca_api.alpaca_api import AlpacaApi
from backtester.backtester_lite import BacktesterLite
from time import sleep
import pandas as pd

live = False

alp = AlpacaApi()
today = datetime.now()
end = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
start = (datetime.now()-timedelta(days=1) - timedelta(days=10)).strftime("%Y-%m-%d")

sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",attrs={"id":"constituents"})[0]
sp500.rename(columns={"Symbol":"ticker"},inplace=True)

stock_parameter = {
 
 'strategy': 'window',
 'value': True,
 'lookback': 5,
 'holding_period': 1,
 'floor': -10,
 'ceiling': 10,
 'volatility': 1,
 "local_min":False,
 "asset":"stocks",
 "positions":20,
 "allocation":1

}


crypto_parameter = {
    
 'strategy': 'rolling',
 'value': False,
 'lookback': 5,
 'holding_period': 1,
 'floor': 0,
 'ceiling': 10,
 'volatility': 0.5,
 'local_min': False

 }

parameters = []
# parameters.append(crypto_parameter)
parameters.append(stock_parameter)

iteration = 1
if today.weekday() < 5:
    if live:
        try:
            print("closing")
            closed_orders = alp.live_close_all()
            sleep(300)
        except Exception as e:
            print(str(e))

    account = alp.live_get_account()
    cash = float(account.cash) * 0.995

    for parameter in parameters:
        try:
            asset = parameter["asset"]
            positions = parameter["positions"]
            tickers = sp500["ticker"] if asset == "stocks" else ["BTC/USD"]
            lookback = parameter["lookback"]
            allocation = parameter["allocation"]
            simulation = []
            for ticker in tickers:
                try:
                    if asset == "stocks":
                        example = alp.get_ticker_data(ticker,start,end)
                        ticker_data = example.df.reset_index().rename(columns={"symbol":"ticker","timestamp":"date","close":"adjclose"})[["date","ticker","adjclose"]]
                    else:
                        example = alp.get_crypto_data(ticker,start,end)
                        ticker_data = example.df.reset_index().rename(columns={"timestamp":"date","close":"adjclose"})[["date","adjclose"]]
                        ticker_data["ticker"] = ticker

                    ticker_data = p.column_date_processing(ticker_data)
                    ticker_data.sort_values("date",inplace=True)
                    ticker_data["week"] = [x.week for x in ticker_data["date"]]
                    ticker_data["day"] = [x.weekday() for x in ticker_data["date"]]
                    ticker_data["prev_close"] = ticker_data["adjclose"]
                    ticker_data["d1"] = ticker_data[f"adjclose"].pct_change()
                    ticker_data[f"window_{lookback}"] = ticker_data["prev_close"].shift(lookback)
                    ticker_data[f"rolling_{lookback}"] = ticker_data["prev_close"].rolling(lookback).mean()
                    ticker_data[f"rolling_stdev_{lookback}"] = ticker_data["prev_close"].rolling(lookback).std()
                    ticker_data[f"rolling_pct_stdev_{lookback}"] = ticker_data[f"rolling_stdev_{lookback}"] / ticker_data[f"rolling_{lookback}"]
                    simulation.append(ticker_data.dropna())
                except Exception as e:
                    print(ticker,str(e))

            final = pd.concat(simulation)
            final = final[final["date"]==final["date"].max()]

            trades = BacktesterLite.backtest(positions,final.copy(),iteration,parameter,True)

            algo_cash = float(cash * allocation)

            if trades.index.size > 0:
                for row in trades.iterrows():
                    try:
                        ticker = row[1]["ticker"]
                        amount = round(algo_cash / positions,2)
                        if live:
                            alp.live_market_order(ticker,amount)
                            print(ticker,amount)
                        else:
                            print(ticker,amount)
                    except Exception as e:
                        print(str(e))

        except Exception as e:
            print(str(e))
else:
    print("weekend!")