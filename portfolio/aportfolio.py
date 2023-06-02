from pricer.pricer_factory import PricerFactory as pricer_fact
from classifier.classifier_factory import ClassifierFactory as classifier_fact
from ranker.ranker_factory import RankerFactory as ranker_fact
from modeler_strats.universal_modeler import UniversalModeler
from backtester.quarterly_backtester import QuarterlyBacktester
from backtester.weekly_backtester import WeeklyBacktester

class APortfolio(object):

    def __init__(self,pricer,classifier,ranker):
        self.pricer = pricer
        self.classifier = classifier
        self.ranker = ranker
        self.modeler_strat = UniversalModeler()

    def initialize(self,model_start_date,model_end_date,current_start_date):
        self.model_start_date = model_start_date
        self.model_end_date = model_end_date
        self.current_start_date = current_start_date
        self.pricer_class = pricer_fact.build(self.pricer)
        self.ranker_class = ranker_fact.build(self.ranker)
        self.classifier_class = classifier_fact.build(self.classifier)
    
    def create_training_sets(self):
        self.create_pricer_training_set()
        self.create_classifier_training_set()
        self.create_ranker_training_set()

    def create_pricer_training_set(self):
        self.pricer_class.training_set()
    
    def create_classifier_training_set(self):
        if self.classifier_class == None:
            return
        
    def create_ranker_training_set(self):
        if self.ranker_class == None:
            return
 
    def create_historical_models(self):
        self.create_pricer_historical_model()
        self.create_classifier_historical_model()
        self.create_ranker_historical_model()
    
    def create_pricer_historical_model(self):
        self.pricer_class.db.connect()
        if self.pricer_class.isai:
            data = self.pricer_class.db.retrieve("historical_training_set")
            self.pricer_class.db.drop("sim")
            for year in range(self.model_start_date.year,self.model_end_date.year):
                training_slice = data[(data["year"]<year) & (data["year"] >= year - 4)].reset_index(drop=True)
                prediction_set = data[data["year"]==year].reset_index(drop=True)
                stuff = self.modeler_strat.model(training_slice,prediction_set,self.pricer_class.factors,False)
                stuff = stuff.rename(columns={"prediction":f"price_prediction"})
                relevant_columns = list(set(list(stuff.columns)) - set(self.pricer_class.factors))
                stuff = self.pricer_class.sim_processor(stuff)
                self.pricer_class.db.store("sim",stuff[relevant_columns])
        self.pricer_class.db.disconnect()
    
    def create_classifier_historical_model(self):
        if self.classifier_class == None:
            return
        
    def create_ranker_historical_model(self):
        if self.ranker_class == None:
            return
    
    def create_simulation(self):
        pricer_sim = self.pull_pricer_sim()
        classifier_sim = self.pull_classifier_sim()
        ranker_sim = self.pull_ranker_sim()
        if ranker_sim == None:
            if classifier_sim == None:
                return pricer_sim
        return pricer_sim
    
    def pull_pricer_sim(self):
        self.pricer_class.db.connect()
        sim = self.pricer_class.db.retrieve("sim")
        self.pricer_class.db.disconnect()
        return sim
    
    def pull_classifier_sim(self):
        if self.classifier_class == None:
            return None
        
    def pull_ranker_sim(self):
        if self.ranker_class == None:
            return None
    
        # for self.pricer_class in self.strat_classes:

        #             if strat_class.classification:
        #                 classification_training_slice = classification_data[(classification_data["year"]<year) & (classification_data["year"] >= year - 4)].reset_index(drop=True)
        #                 classification_prediction_set = classification_data[classification_data["year"]==year].reset_index(drop=True)
        #                 stuff = self.model_strat.classification_model(classification_training_slice,classification_prediction_set,strat_class.classification_factors,False)
        #                 stuff = stuff.rename(columns={"prediction":f"{strat_class.name}_classification_prediction"})
        #                 class_relevant_columns = list(set(list(stuff.columns)) - set(strat_class.classification_factors))
        #                 strat_class.db.store("classification_sim",stuff[class_relevant_columns])
        #     else:
        #         continue
        #     strat_class.db.disconnect()
                
    # def historical_backtest(self):
    #     current = False
    #     for strat_class in self.strat_classes:
    #         if strat_class.quarterly:
    #             b = QuarterlyBacktester(strat_class,current,strat_class.positions,self.model_start_date,self.model_end_date)
    #         else:
    #             b = WeeklyBacktester(strat_class,current,strat_class.positions,self.model_start_date,self.model_end_date)
    #         price_returns = b.stock_returns(self.market,self.sec,self.sp500)
    #         simulation = strat_class.pull_sim()
    #         if strat_class.isai:
    #             if strat_class.classification:
    #                 classification_simulation = strat_class.pull_classification_sim()
    #                 simulation = simulation.merge(classification_simulation,on=["year",strat_class.group_timeframe,"ticker"],how="left")
    #         if strat_class.quarterly:
    #             simulation["year"] = simulation["year"] + 1
    #         else:
    #             simulation["week"] = simulation["week"] + 1
    #         simulation = b.create_sim(simulation,price_returns)
    #         parameters = b.create_parameters()
    #         b.backtest(parameters,simulation,self.sp500)
    
    # def pull_trades(self):
    #     t = []
    #     for strat_class in self.strat_classes:
    #         strat_class.db.connect()
    #         trade = strat_class.db.retrieve("trades")
    #         strat_class.db.disconnect()
    #         trade["strat"] = strat_class.name
    #         trade["positions"] = strat_class.positions
    #         t.append(trade)
    #     return t