from database.market import Market
from database.sec import SEC
from modeler_strats.universal_modeler import UniversalModeler
from strategy.strategy_factory import StratFactory as strat_fact
from backtester.quarterly_backtester import QuarterlyBacktester
from backtester.weekly_backtester import WeeklyBacktester

class Fund(object):

    def __init__(self,strats,model_start_date,model_end_date,current_start_date):
        self.market = Market()
        self.sec = SEC()
        self.model_start_date = model_start_date
        self.model_end_date = model_end_date
        self.current_start_date = current_start_date
        self.strats = strats
        self.model_strat = UniversalModeler()
        self.pull_sp500()
    
    def pull_sp500(self):
        self.market.connect()
        self.sp500 = self.market.retrieve("sp500")
        self.sp500 = self.sp500.rename(columns={"Symbol":"ticker"})
        self.market.disconnect()

    def initialize_strats(self):
        strat_classes = []
        for strat in self.strats:
            strat_classes.append(strat_fact.build_strat(strat))
        self.strat_classes = strat_classes

    def create_training_sets(self):
        for strat_class in self.strat_classes:
            self.market.connect()
            self.sec.connect()
            if strat_class.isai:
                training_set = strat_class.training_set(self.market,self.sec,self.sp500)
                strat_class.db.connect()
                strat_class.db.drop("historical_training_set")
                strat_class.db.store("historical_training_set",training_set)
                strat_class.db.disconnect()
                if strat_class.classification:
                    training_set = strat_class.classification_training_set(self.market,self.sec,self.sp500)
                    strat_class.db.connect()
                    strat_class.db.drop("historical_classification_training_set")
                    strat_class.db.store("historical_classification_training_set",training_set)
                    strat_class.db.disconnect()
            else:
                sim = strat_class.create_sim(self.market,self.sec,self.sp500)
                strat_class.db.connect()
                strat_class.db.drop("sim")
                strat_class.db.store("sim",sim)
                strat_class.db.disconnect()
            self.sec.disconnect()
            self.market.disconnect()
 
    def historical_model(self):
        for strat_class in self.strat_classes:
            strat_class.db.connect()
            if strat_class.isai:
                data = strat_class.db.retrieve("historical_training_set")
                if strat_class.classification:
                        classification_data = strat_class.db.retrieve("historical_classification_training_set")
                strat_class.db.drop("sim")
                strat_class.db.drop("classification_sim")
                for year in range(self.model_start_date.year,self.model_end_date.year):
                    training_slice = data[(data["year"]<year) & (data["year"] >= year - 4)].reset_index(drop=True)
                    prediction_set = data[data["year"]==year].reset_index(drop=True)
                    stuff = self.model_strat.model(training_slice,prediction_set,strat_class.factors,False)
                    stuff = stuff.rename(columns={"prediction":f"{strat_class.name}_prediction"})
                    relevant_columns = list(set(list(stuff.columns)) - set(strat_class.factors))
                    strat_class.db.store("sim",stuff[relevant_columns])
                    if strat_class.classification:
                        classification_training_slice = classification_data[(classification_data["year"]<year) & (classification_data["year"] >= year - 4)].reset_index(drop=True)
                        classification_prediction_set = classification_data[classification_data["year"]==year].reset_index(drop=True)
                        stuff = self.model_strat.classification_model(classification_training_slice,classification_prediction_set,strat_class.classification_factors,False)
                        stuff = stuff.rename(columns={"prediction":f"{strat_class.name}_classification_prediction"})
                        class_relevant_columns = list(set(list(stuff.columns)) - set(strat_class.classification_factors))
                        strat_class.db.store("classification_sim",stuff[class_relevant_columns])
            else:
                continue
            strat_class.db.disconnect()
                
    def historical_backtest(self):
        current = False
        for strat_class in self.strat_classes:
            if strat_class.quarterly:
                b = QuarterlyBacktester(strat_class,current,strat_class.positions,self.model_start_date,self.model_end_date)
            else:
                b = WeeklyBacktester(strat_class,current,strat_class.positions,self.model_start_date,self.model_end_date)
            price_returns = b.stock_returns(self.market,self.sec,self.sp500)
            simulation = strat_class.pull_sim()
            if strat_class.isai:
                if strat_class.classification:
                    classification_simulation = strat_class.pull_classification_sims()
                    simulation = simulation.merge(classification_simulation,on=["year",strat_class.group_timeframe,"ticker"],how="left")
            if strat_class.quarterly:
                simulation["year"] = simulation["year"] + 1
            else:
                simulation["week"] = simulation["week"] + 1
            simulation = b.create_sim(simulation,price_returns)
            parameters = b.create_parameters()
            b.backtest(parameters,simulation,self.sp500)
    
    def pull_trades(self):
        t = []
        for strat_class in self.strat_classes:
            strat_class.db.connect()
            trade = strat_class.db.retrieve("trades")
            strat_class.db.disconnect()
            trade["strat"] = strat_class.name
            trade["positions"] = strat_class.positions
            t.append(trade)
        return t