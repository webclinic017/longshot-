from tradealgorithm.tradealgorithm import TradeAlgorithm
from tradealgorithm.tradealgorithmfactory import TradeAlgorithmFactory
from pricer.pricer import Pricer
from ranker.ranker import Ranker
from classifier.classifier import Classifier
from datetime import datetime
from tqdm import tqdm

trade_algo = TradeAlgorithmFactory.build(TradeAlgorithm.RRR_BETA)

ranker_class = Ranker.NONE
classifier_class = Classifier.NONE
current = False

start = datetime(2020,1,1)
end = datetime.now()
current_date = datetime.now()

pricers = []
pricers.append(Pricer.DAILY_STOCK_WINDOW)
pricers.append(Pricer.DAILY_STOCK_ROLLING)
pricers.append(Pricer.WEEKLY_STOCK_WINDOW)
pricers.append(Pricer.WEEKLY_STOCK_ROLLING)
pricers.append(Pricer.MONTHLY_STOCK_WINDOW)
pricers.append(Pricer.MONTHLY_STOCK_ROLLING)

for pricer_class in tqdm(pricers):
    try:
        trade_algo.initialize(pricer_class,ranker_class,classifier_class,start,end,current_date)
        trade_algo.initialize_bench_and_yields()
        trade_algo.initialize_classes()
        trade_algo.load_optimal_parameter()
        simulation = trade_algo.create_current_simulation()
        returns = trade_algo.create_returns(True)
        merged = trade_algo.merge_sim_returns(simulation,returns)
        complete = trade_algo.apply_yields(merged,True)
        trade_algo.initialize_backtester()
        trade_algo.drop_recommendations()
        trades = trade_algo.run_recommendation(complete)
    except Exception as e:
        print(trade_algo.name,str(e))