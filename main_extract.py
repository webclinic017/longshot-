import pandas as pd
import matplotlib.pyplot as plt
from database.market import Market
from database.adatabase import ADatabase
from datetime import datetime, timedelta
from tqdm import tqdm
from extractor.tiingo_extractor import TiingoExtractor
from extractor.forex_extractor import FOREXExtractor
from extractor.fred_extractor import FREDExtractor
# from time import sleep
from processor.processor import Processor as p

market = Market()
sp500 = ADatabase("sp500")
market.connect()
sp5 = market.retrieve("sp500")
market.disconnect()

market.connect()
start = market.retrieve_max_date("stocks")
market.disconnect()
end = datetime.now().strftime("%Y-%m-%d")
print(start,end)

market.connect()
data = FOREXExtractor.extract(start,end)
values = pd.DataFrame(data["rates"].values.tolist())
dates = data["rates"].keys().tolist()
values["dates"]= dates
market.store("forex",values)
market.disconnect()

market.connect()
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
except Exception as e:
    print(str(e))
market.disconnect()

## cloud db update
start = datetime.now() - timedelta(days=800)
market.connect()
stocks = market.retrieve("stocks")
crypto = market.retrieve("crypto")
tyields = market.retrieve("tyields")
bench = market.retrieve("spy")
market.disconnect()
stocks = p.column_date_processing(stocks)
stocks = stocks[stocks["date"]>=start]
crypto = p.column_date_processing(crypto)
crypto = crypto[crypto["date"]>=start]
market.cloud_connect()
market.drop("stocks")
market.drop("tyields")
market.drop("spy")
market.drop("crypto")
market.store("tyields",tyields)
market.store("spy",bench)
market.store("stocks",stocks)
market.create_index("stocks","ticker")
market.store("crypto",crypto)
market.create_index("crypto","ticker")
market.disconnect()