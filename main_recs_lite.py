import pandas as pd
from datetime import datetime,timedelta
from lite.backtester_lite import Backtester
from lite.transformer_lite import Transformer
from alpaca_api.alpaca_api import AlpacaApi
from tqdm import tqdm

alp = AlpacaApi()
parameter = pd.read_csv("./parameter.csv").to_dict("records")[0]
sp100 = pd.read_html("https://en.wikipedia.org/wiki/S%26P_100",attrs={"id":"constituents"})[0]
sp100.rename(columns={"Symbol":"ticker","Sector":"GICS Sector"},inplace=True)

lookback = parameter["lookback"]
current = True

end_date = datetime.now() - timedelta(days=1)
extract_date = end_date - timedelta(days=lookback*2)
start_date = end_date - timedelta(days=lookback*2)

lookbacks = []
lookbacks.append(lookback)
tickers = sp100["ticker"]
positions = len(list(sp100["GICS Sector"].unique()))

simulation = []
for ticker in tqdm(tickers):
    try:
        ticker_data = alp.get_ticker_data(ticker,extract_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
        ticker_data = Transformer.transform(ticker_data,lookbacks,current)
        simulation.append(ticker_data.dropna())
    except Exception as e:
        print(ticker,str(e))

final = pd.concat(simulation).merge(sp100[["ticker","GICS Sector"]],how="left")
final = final[final["date"]==final["date"].max()]
try:
    trades = Backtester.backtest(final.copy(),1,parameter,current)
except Exception as e:
    print(str(e))

trades.to_csv("trades.csv")