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
sp500 = ADatabase("sp500")
market.connect()
sp5 = market.retrieve("sp500")
market.disconnect()

market.connect()
start = market.retrieve_max_date("stocks")
market.disconnect()

# start = start.strftime("%Y-%m-%d")
end = datetime.now().strftime("%Y-%m-%d")

# start = datetime(1999,1,1).strftime("%Y-%m-%d")
# end = datetime(2001,1,1).strftime("%Y-%m-%d")

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
    data = FREDExtractor.tyields2(start,end)
    market.store("tyields2",data)
    data = FREDExtractor.tyields10(start,end)
    market.store("tyields10",data)
except Exception as e:
    print(str(e))
market.disconnect()

## cloud db update
# start = datetime.now() - timedelta(days=800)
# market.connect()
# stocks = market.retrieve("stocks")
# crypto = market.retrieve("crypto")
# tyields = market.retrieve("tyields")
# tyields2 = market.retrieve("tyields2")
# tyields10 = market.retrieve("tyields10")
# bench = market.retrieve("spy")
# market.disconnect()
# stocks = p.column_date_processing(stocks)
# stocks = stocks[stocks["date"]>=start]
# crypto = p.column_date_processing(crypto)
# crypto = crypto[crypto["date"]>=start]
# market.cloud_connect()
# market.drop("stocks")
# market.drop("tyields")
# market.drop("tyields2")
# market.drop("tyields10")
# market.drop("spy")
# market.drop("crypto")
# market.store("tyields",tyields)
# market.store("tyields2",tyields2)
# market.store("tyields10",tyields10)
# market.store("spy",bench)
# market.store("stocks",stocks)
# market.create_index("stocks","ticker")
# market.store("crypto",crypto)
# market.create_index("crypto","ticker")
# market.disconnect()