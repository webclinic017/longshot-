from tradealgorithm.tradealgorithm import TradeAlgorithm
from tradealgorithm.tradealgorithmfactory import TradeAlgorithmFactory
from pricer.pricer import Pricer
from ranker.ranker import Ranker
from classifier.classifier import Classifier
from datetime import datetime
from tqdm import tqdm
from alpaca_api.alpaca_api import AlpacaApi
from time import sleep
import pandas as pd
import json

trade_algo = TradeAlgorithmFactory.build(TradeAlgorithm.RRR_BETA)
alp = AlpacaApi()

ranker_class = Ranker.NONE
classifier_class = Classifier.NONE
current = False

start = datetime(2023,1,1)
end = datetime.now()
current_date = datetime.now()
week = current_date.isocalendar()[1]
positions = 20

pricer_classes = [] 
# pricer_classes.append(Pricer.WEEKLY_STOCK_WINDOW)
pricer_classes.append(Pricer.DAILY_STOCK_ROLLING)

for pricer_class in tqdm(pricer_classes):
    try:
        trade_algo.initialize(pricer_class,ranker_class,classifier_class,start,end,current_date)
        trade_algo.initialize_classes()
        final = trade_algo.pull_recommendations()
        final = final.sort_values("")
        if final.index.size > 0:
            account = alp.live_get_account()
            cash = float(account.cash)
            # executing order
            order_data = []
            for row in final.iterrows():
                try:
                    ticker = "BTC/USD" if row[1]["ticker"] == "BTC" else row[1]["ticker"] 
                    amount = round(cash / positions,2)
                    print(ticker,amount)
                    order_data.append(alp.live_market_order(ticker,amount))
                    sleep(1)
                except Exception as e:
                    trade_algo.db.connect()
                    trade_algo.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"error":str(e)}]))
                    trade_algo.db.disconnect()
            order_dicts = pd.DataFrame([json.loads(order_d.json()) for order_d in order_data])
            trade_algo.db.cloud_connect()
            trade_algo.db.store("orders",order_dicts)
            trade_algo.db.disconnect()
        trade_algo.db.cloud_connect()
        trade_algo.db.store("algo_iterations",pd.DataFrame([{"date":str(datetime.now()),"status":"complete"}]))
        trade_algo.db.disconnect()
    except Exception as e:
            trade_algo.db.cloud_connect()
            trade_algo.db.store("algo_iterations",pd.DataFrame([{"date":str(datetime.now()),"status":"incomplete"}]))
            trade_algo.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"trade","error":str(e)}]))
            trade_algo.db.disconnect()