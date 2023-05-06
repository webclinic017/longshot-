from modeler.modeler import Modeler as m
import pandas as pd

class CategoryModeler(object):
    def model(self,start_year,end_year):
        sim = []
        for year in range(start_year,int(end_year)):
            try:
                data = self.pull_training_data()
                training_set = self.strat_class.training_set(data,year)
                training_set = training_set.sample(frac=1)
                prediction_set = self.strat_class.prediction_set(data,year)
                refined = {"X":training_set[self.strat_class.factors],"y":training_set["y"]}
                models = m.regression(refined)
                prediction_set = m.predict(models,prediction_set,self.strat_class.factors)
                prediction_set = self.strat_class.prediction_clean(prediction_set)
                sim.append(prediction_set)
            except Exception as e:
                print(str(e))
                continue
        return pd.concat(sim)