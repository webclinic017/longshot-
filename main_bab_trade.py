from time import sleep
from datetime import datetime, timezone, timedelta
from alpaca_api.alpaca_api import AlpacaApi

from main_bab_fund import MainBabFund as mf

import pandas as pd
import json

alp = AlpacaApi()

fund = mf.load_fund()
new_york_date = datetime.now(tz=timezone(offset=timedelta(hours=-4)))
year = new_york_date.year

for portfolio in fund.portfolios:
    try:
        ## setup
        pricer_class = portfolio.pricer_class
        positions = 10 if pricer_class.asset_class.value == "stocks" else 1
        portfolio.db.cloud_connect()
        final = portfolio.db.retrieve("recs")
        portfolio.db.disconnect()
        ## buys
        if final.index.size > 0:
            portfolio.db.cloud_connect()
            portfolio.db.store("proposals",final)
            portfolio.db.disconnect()
            account = alp.live_get_account()
            cash = float(account.cash)
            # executing order
            order_data = []
            for row in final.iterrows():
                try:
                    ticker = "BTC/USD" if row[1]["ticker"] == "BTC" else row[1]["ticker"] 
                    amount = round(cash / (positions)) 
                    order_data.append(alp.live_market_order(ticker,amount))
                    sleep(1)
                except Exception as e:
                    portfolio.db.cloud_connect()
                    portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"error":str(e)}]))
                    portfolio.db.disconnect()
            order_dicts = pd.DataFrame([json.loads(order_d.json()) for order_d in order_data])
            portfolio.db.cloud_connect()
            portfolio.db.store("orders",order_dicts)
            portfolio.db.disconnect()
        
        # logging
        portfolio.db.cloud_connect()
        portfolio.db.store("iterations",pd.DataFrame([{"date":str(datetime.now()),"status":"complete"}]))
        portfolio.db.disconnect()
        sleep(300)

    except Exception as e:
            portfolio.db.cloud_connect()
            portfolio.db.store("iterations",pd.DataFrame([{"date":str(datetime.now()),"status":"incomplete"}]))
            portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"trade","error":str(e)}]))
            portfolio.db.disconnect()