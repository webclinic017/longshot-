import pandas as pd
from database.market import Market
from database.adatabase import ADatabase
from datetime import datetime, timedelta
from tqdm import tqdm
from extractor.tiingo_extractor import TiingoExtractor
from extractor.forex_extractor import FOREXExtractor
from extractor.fred_extractor import FREDExtractor
from processor.processor import Processor as p

market = Market()

sp5 = pd.read_csv("sp500.csv")
sp5 = sp5.drop("Unnamed: 0",axis=1)

market.connect()
market.store("sp500",sp5)
market.disconnect()

# market.connect()
# sp5 = market.retrieve("sp500")
# market.disconnect()

# market.connect()
# start = datetime(2020,1,1).strftime("%Y-%m-%d")
# market.disconnect()
# end = datetime.now().strftime("%Y-%m-%d")

market.connect()
start = market.retrieve_max_date("stocks")
market.disconnect()
end = datetime.now().strftime("%Y-%m-%d")

market.cloud_connect()
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

market.cloud_connect()
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

market.cloud_connect()
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