from time import sleep
from datetime import datetime, timezone, timedelta
from alpaca_api.alpaca_api import AlpacaApi

from database.market import Market
from datetime import datetime
from fund.fund import Fund

from pricer.pricer import Pricer as pricer_list
from ranker.ranker import Ranker as ranker_list
from classifier.classifier import Classifier as classifier_list
from portfolio.aportfolio import APortfolio
import pandas as pd

from time import sleep

import json

market = Market()
positions = 20
alp = AlpacaApi()


start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
end = datetime.now().strftime("%Y-%m-%d")

portfolio = APortfolio(pricer_list.WEEKLY_STOCK_SPECULATION
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolio_ii = APortfolio(pricer_list.WEEKLY_CRYPTO_SPECULATION
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolios = [portfolio,portfolio_ii]

fund = Fund(portfolios,start,end,end)

fund.initialize_portfolios()
new_york_date = datetime.now(tz=timezone(offset=timedelta(hours=-4)))
year = new_york_date.year
week = new_york_date.isocalendar()[1]
try:
        
    portfolio.db.cloud_connect()
    final = portfolio.db.retrieve("recs")
    portfolio.db.disconnect()
    final = final[final["week"]==week]
    closed_orders = alp.paper_close_all()
    closed_order_df = pd.DataFrame([json.loads(closed_order.json())["body"] for closed_order in closed_orders])
    
    if closed_order_df.index.size > 1:
        portfolio.db.cloud_connect()
        portfolio.db.store("orders",closed_order_df)
        portfolio.db.disconnect()
    if final.index.size > 0:
        account = alp.paper_get_account()
        cash = float(account.cash) - 10
        # executing order
        order_data = []
        for row in final.iterrows():
            try:
                ticker = row[1]["ticker"]
                amount = round(cash / positions) 
                order_data.append(alp.paper_market_order(ticker,amount))
                sleep(1)
            except Exception as e:
                portfolio.db.cloud_connect()
                portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"error":str(e)}]))
                portfolio.db.disconnect()
        order_dicts = pd.DataFrame([json.loads(order_d.json()) for order_d in order_data])
        portfolio.db.cloud_connect()
        portfolio.db.store("orders",order_dicts)
        portfolio.db.disconnect()

    portfolio.db.cloud_connect()
    portfolio.db.store("iterations",pd.DataFrame([{"date":str(datetime.now()),"status":"complete"}]))
    portfolio.db.disconnect()
    
except Exception as e:
        portfolio.db.cloud_connect()
        portfolio.db.store("iterations",pd.DataFrame([{"date":str(datetime.now()),"status":"incomplete"}]))
        portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"error":str(e)}]))
        portfolio.db.disconnect()