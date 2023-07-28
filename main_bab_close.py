from time import sleep
from datetime import datetime, timezone, timedelta
from alpaca_api.alpaca_api import AlpacaApi

import pandas as pd
import json
from main_bab_fund import MainBabFund as mf

alp = AlpacaApi()

fund = mf.load_fund()


new_york_date = datetime.now(tz=timezone(offset=timedelta(hours=-4)))
year = new_york_date.year
week = new_york_date.isocalendar()[1]

for portfolio in fund.portfolios:
    try:
        pricer_class = portfolio.pricer_class
        asset_class = pricer_class.asset_class.value
        if week % (portfolio.parameter["sell_day"] / 5) == 0:
            if asset_class == "stocks":
                closed_orders = alp.live_close_all()
            closed_order_df = pd.DataFrame([json.loads(closed_order.json())["body"] for closed_order in closed_orders])
            portfolio.db.cloud_connect()
            portfolio.db.store("orders",closed_order_df)
            portfolio.db.disconnect()
        else:
             continue
    except Exception as e:
            portfolio.db.cloud_connect()
            portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"trade","error":str(e)}]))
            portfolio.db.disconnect()

sleep(300)