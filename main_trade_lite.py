from datetime import datetime, timedelta
from processor.processor import Processor as p
from alpaca_api.alpaca_api import AlpacaApi
from lite.backtester_lite import BacktesterLite
from time import sleep
import pandas as pd

live = True

alp = AlpacaApi()
today = datetime.now()
end = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
start = (datetime.now()-timedelta(days=1) - timedelta(days=10)).strftime("%Y-%m-%d")

sp100 = pd.read_html("https://en.wikipedia.org/wiki/S%26P_100",attrs={"id":"constituents"})[0]
sp100.rename(columns={"Symbol":"ticker","Symbol":"GICS Sector"},inplace=True)

parameter = pd.read_csv("./parameter.csv").drop("Unnamed: 0",axis=1).to_dict("records")[0]
iteration = 1
positions = len(list(sp100["GICS Sector"].unique()))
asset = "stocks"

if today.weekday() < 5:
    try:
        tickers = sp100["ticker"]
        lookback = parameter["lookback"]
        simulation = []
        for ticker in tickers:
            try:
                # if asset == "stocks":
                ticker_data = alp.get_ticker_data(ticker,start,end)
                # else:
                #     ticker_data = alp.get_crypto_data(ticker,start,end)
                #     ticker_data["ticker"] = ticker

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
        final = pd.concat(simulation).merge(sp100[["ticker","GICS Sector"]],how="left")
        final = final[final["date"]==final["date"].max()]

        trades = BacktesterLite.backtest(sp100,final.copy(),iteration,parameter,True)
        if live:
            account = alp.live_get_account()
            cash = float(account.cash) * 0.999
            closed_orders = alp.live_close_all()
            sleep(300)
            if trades.index.size > 0:
                for row in trades.iterrows():
                    try:
                        ticker = row[1]["ticker"]
                        amount = round(cash / positions,2)
                        print(ticker,amount)
                        alp.live_market_order(ticker,amount)
                    except Exception as e:
                        print(str(e))
                        continue
                    
    except Exception as e:
        print(str(e))
else:
    print("weekend!")