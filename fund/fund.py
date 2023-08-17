from strategy.strategy import Strategy
from strategy.strategyfactory import StrategyFactory
from pricer.pricer import Pricer
from ranker.ranker import Ranker
from classifier.classifier import Classifier
from database.adatabase import ADatabase
class Fund(object):

    def __init__(self,start,end,current_date):
        self.name = "delta"
        self.start = start
        self.end = end
        self.current_date = current_date
        self.db = ADatabase("delta")

    def initialize(self):
        self.strategies = []
        self.ranker_classes = [Ranker.QUARTERLY_STOCK_EARNINGS_RANKER,Ranker.QUARTERLY_STOCK_ROLLING_RANKER]
        self.classifier_classes = [Classifier.QUARTERLY_STOCK_ROLLING_CLASSIFIER]
        self.pricer_classes = [] 
        self.pricer_classes.append(Pricer.DAILY_STOCK_ROLLING)
        self.pricer_classes.append(Pricer.WEEKLY_STOCK_ROLLING)
        self.pricer_classes.append(Pricer.QUARTERLY_STOCK_ROLLING)
        self.pricer_classes.append(Pricer.DAILY_STOCK_WINDOW)
        self.pricer_classes.append(Pricer.WEEKLY_STOCK_WINDOW)
        self.pricer_classes.append(Pricer.QUARTERLY_STOCK_WINDOW)

        for pricer_class in self.pricer_classes:
            for ranker_class in self.ranker_classes:
                for classifier_class in self.classifier_classes:
                    try:
                        strategy = StrategyFactory.build(Strategy.RRR_BETA)
                        strategy.initialize(pricer_class,ranker_class,classifier_class,self.start,self.end,self.current_date)
                        strategy.initialize_bench_and_yields()
                        strategy.initialize_classes()
                        self.strategies.append(strategy)
                    except Exception as e:
                        print(str(e))