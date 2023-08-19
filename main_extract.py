import pandas as pd
from database.market import Market
from datetime import datetime, timedelta
from tqdm import tqdm
from extractor.tiingo_extractor import TiingoExtractor
from extractor.forex_extractor import FOREXExtractor
from extractor.fred_extractor import FREDExtractor
from processor.processor import Processor as p
from alpaca_api.alpaca_api import AlpacaApi
from time import sleep

alp = AlpacaApi()
market = Market()

sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",attrs={"id":"constituents"})[0].rename(columns={"Symbol":"ticker"})

market.connect()
market.drop("sp500")
market.store("sp500",sp500)
market.disconnect()

sp100 = pd.read_html("https://en.wikipedia.org/wiki/S%26P_100",attrs={"id":"constituents"})[0].rename(columns={"Symbol":"ticker","Sector":"GICS Sector"})

market.connect()
market.drop("sp100")
market.store("sp100",sp100)
market.disconnect()

start = (datetime.now() -timedelta(days=365.25*7)).strftime("%Y-%m-%d")
end = (datetime.now() -timedelta(days=1)).strftime("%Y-%m-%d")

market.connect()
market.drop("stocks")
market.create_index("stocks","ticker")
for ticker in tqdm(list(sp500["ticker"].unique())):
    try:
        data = alp.get_ticker_data(ticker,start,end)
        data["ticker"] = ticker
        market.store("stocks",data)
        sleep(1)
    except Exception as e:
        print(ticker,print(str(e)))
market.disconnect()

# market.connect()
# market.drop("tiingo_stocks")
# market.create_index("tiingo_stocks","ticker")
# for ticker in tqdm(list(sp500["ticker"].unique())):
#     try:
#         if "." in ticker:
#             ticker = ticker.replace(".","-")
#         try:
#             data = TiingoExtractor.extract(ticker,start,end)
#             data["ticker"] = ticker
#             market.store("tiingo_stocks",data)
#             sleep(1)
#         except Exception as e:
#             print(ticker,print(str(e)))
#     except Exception as e:
#         print(ticker,str(e))
# market.disconnect()

# market.connect()
# try:
#     try:
#         data = TiingoExtractor.crypto("BTC",start,end)
#         data["ticker"] = 'BTC'
#         market.store("crypto",data)
#     except:
#         print(ticker,"tiingo")
# except Exception as e:
#     print(ticker,str(e))
# market.disconnect()

# market.connect()
# data = FOREXExtractor.extract(start,end)
# values = pd.DataFrame(data["rates"].values.tolist())
# dates = data["rates"].keys().tolist()
# values["dates"]= dates
# market.store("forex",values)
# market.disconnect()

# market.connect()
# try:
#     data = FREDExtractor.spy(start,end)
#     market.store("spy",data)
#     data = FREDExtractor.tyields(start,end)
#     market.store("tyields",data)
#     data = FREDExtractor.tyields2(start,end)
#     market.store("tyields2",data)
#     data = FREDExtractor.tyields10(start,end)
#     market.store("tyields10",data)
# except Exception as e:
#     print(str(e))
# market.disconnect()

# cloud db update
# start = datetime.now() - timedelta(days=260)
# market.connect()
# stocks = market.retrieve("stocks")
# market.disconnect()

# stocks = p.column_date_processing(stocks)
# stocks = stocks[stocks["date"]>=start]
# market.cloud_connect()
# market.drop("stocks")
# market.store("stocks",stocks)
# market.create_index("stocks","ticker")
# market.drop("sp500")
# market.store("sp500",sp5)
# market.disconnect()