import pandas as pd
from database.market import Market
from datetime import datetime, timedelta
from tqdm import tqdm
from alpaca_api.alpaca_api import AlpacaApi

alp = AlpacaApi()
market = Market()

sp100 = pd.read_html("https://en.wikipedia.org/wiki/S%26P_100",attrs={"id":"constituents"})[0].rename(columns={"Symbol":"ticker","Sector":"GICS Sector"})
sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",attrs={"id":"constituents"})[0].rename(columns={"Symbol":"ticker"})
sp100 = sp100.merge(sp500[["ticker","CIK"]],on="ticker",how="left")

market.connect()
market.drop("sp100")
market.store("sp100",sp100)
market.disconnect()

start = (datetime.now() -timedelta(days=365.25*8)).strftime("%Y-%m-%d")
end = (datetime.now() -timedelta(days=1)).strftime("%Y-%m-%d")

market.connect()
market.drop("stocks")
market.create_index("stocks","ticker")
for ticker in tqdm(list(sp100["ticker"].unique())):
    try:
        data = alp.get_ticker_data(ticker,start,end)
        data["ticker"] = ticker
        market.store("stocks",data)
    except Exception as e:
        print(ticker,print(str(e)))
market.disconnect()