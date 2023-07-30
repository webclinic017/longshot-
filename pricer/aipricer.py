from pricer.atradingpricer import ATradingPricer
from modeler_strats.universal_modeler import UniversalModeler
from datetime import datetime, timedelta
import pickle
import pandas as pd

class AIPricer(ATradingPricer):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = True
        self.modeler = UniversalModeler()
    
    def initialize_model_dates(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date

    def pull_models(self):
        self.db.connect()
        models = self.db.retrieve("models")
        self.db.disconnect()
        return models
    
    def drop_models(self):
        self.db.connect()
        self.db.drop("models")
        self.db.disconnect()
        
    def create_sim(self):
        sims = []
        training_year_offset = self.time_horizon_class.model_offset 
        self.db.connect()
        for year in range(self.start_date.year, self.end_date.year):
            try:
                training_slice = self.training_data[(self.training_data["year"]<year) & (self.training_data["year"] >= year - training_year_offset)].reset_index(drop=True)
                prediction_set = self.training_data[self.training_data["year"]==year].reset_index(drop=True)
                stuff = self.modeler.model(training_slice,prediction_set,self.factors,False)
                stuff = stuff.rename(columns={"prediction":f"price_prediction"})
                relevant_columns = list(set(list(stuff.columns)) - set(self.factors))
                complete = stuff[relevant_columns]
                complete = self.sim_processor(complete)
                self.db.store("sim",complete)
                sims.append(complete)
            except Exception as e:
                print(year,str(e))
        self.db.disconnect()
        return pd.concat(sims)
    
    def create_model(self):
        training_year_offset = self.time_horizon_class.model_offset
        current_year = datetime.now().year
        training_slice = self.training_data[(self.training_data["year"]<current_year) & (self.training_data["year"] >= current_year - training_year_offset)].reset_index(drop=True)
        stuff = self.modeler.recommend_model(training_slice,self.factors,False)
        stuff["model"] = [pickle.dumps(x) for x in stuff["model"]]
        stuff["year"] = current_year
        self.db.connect()
        self.db.store("models",stuff)
        self.db.disconnect()
        return stuff

    def sim_processor(self,sim):
        match self.time_horizon_class.name:
            case "week", "year":
                sim[self.time_horizon_class.naming_convention] =  sim[self.time_horizon_class.naming_convention] + 1
            case "date":
                sim["date"] = sim["date"] + timedelta(days=1)
            case "month":
                sim["month"] = [x+1  if x < 12 else 1 for x in sim["month"]]
            case "quarter":
                sim["quarter"] = [x+1  if x < 4 else 1 for x in sim["quarter"]]
            case _:
                sim = None
        return sim