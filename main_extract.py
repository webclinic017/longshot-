import pandas as pd
from database.market import Market
from datetime import datetime, timedelta
from tqdm import tqdm
from extractor.tiingo_extractor import TiingoExtractor
from extractor.forex_extractor import FOREXExtractor
from extractor.fred_extractor import FREDExtractor
from processor.processor import Processor as p

sp5 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",attrs={"id":"constituents"})[0]

market = Market()

market.connect()
market.drop("sp500")
market.store("sp500",sp5)
market.disconnect()

start = datetime(1995,7,10).strftime("%Y-%m-%d")
end = datetime.now().strftime("%Y-%m-%d")

market.connect()
data = FOREXExtractor.extract(start,end)
values = pd.DataFrame(data["rates"].values.tolist())
dates = data["rates"].keys().tolist()
values["dates"]= dates
market.store("forex",values)
market.disconnect()

market.connect()
market.drop("stocks")
for ticker in tqdm(list(sp5["Symbol"].unique())):
    try:
        if "." in ticker:
            ticker = ticker.replace(".","-")
        try:
            data = TiingoExtractor.extract(ticker,start,end)
            data["ticker"] = ticker
            market.store("stocks",data)
        except:
            print(ticker,"tiingo")
    except Exception as e:
        print(ticker,str(e))
market.create_index("stocks","ticker")
market.disconnect()

market.connect()
try:
    try:
        data = TiingoExtractor.crypto("BTC",start,end)
        data["ticker"] = 'BTC'
        market.store("crypto",data)
    except:
        print(ticker,"tiingo")
except Exception as e:
    print(ticker,str(e))
market.disconnect()

market.connect()
try:
    data = FREDExtractor.spy(start,end)
    market.store("spy",data)
    data = FREDExtractor.tyields(start,end)
    market.store("tyields",data)
    data = FREDExtractor.tyields2(start,end)
    market.store("tyields2",data)
    data = FREDExtractor.tyields10(start,end)
    market.store("tyields10",data)
except Exception as e:
    print(str(e))
market.disconnect()

# cloud db update
start = datetime.now() - timedelta(days=260)
market.connect()
stocks = market.retrieve("stocks")
market.disconnect()
stocks = p.column_date_processing(stocks)
stocks = stocks[stocks["date"]>=start]
market.cloud_connect()
market.drop("stocks")
market.store("stocks",stocks)
market.create_index("stocks","ticker")
market.disconnect()