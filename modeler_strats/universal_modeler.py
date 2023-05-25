from modeler.modeler import Modeler as m
import pickle

class UniversalModeler(object):

    def __init__(self):
        self.name = "universal"
    
    def model(self,training_set,prediction_set,factors,tf):
        refined = {"X":training_set[factors],"y":training_set["y"]}
        models = m.regression(refined,tf)
        prediction_set = m.predict(models,prediction_set,factors)
        if tf:
            prediction_set["prediction"] = (prediction_set["cat_prediction"] + prediction_set["xgb_prediction"] + prediction_set["tf_prediction"]) / 3
        else:
            prediction_set["prediction"] = (prediction_set["cat_prediction"] + prediction_set["xgb_prediction"]) / 2
        return prediction_set
    
    def classification_model(self,training_set,prediction_set,factors,multioutput):
        refined = {"X":training_set[factors],"y":training_set["y"]}
        models = m.classification(refined,multioutput=multioutput)
        prediction_set = m.predict(models,prediction_set,factors)
        prediction_set["prediction"] = ((prediction_set["xgb_prediction"] + prediction_set["cat_prediction"] + prediction_set["tf_prediction"]) / 3) > 0.5
        return prediction_set
    
   
    def recommend_model(self,training_set,factors,tf):
        training_set = training_set.sample(frac=1)
        refined = {"X":training_set[factors],"y":training_set["y"]}
        models = m.regression(refined,tf)
        return models

    def recommend_classification_model(self,training_set,factors,multioutput):
        training_set = training_set.sample(frac=1)
        refined = {"X":training_set[factors],"y":training_set["y"]}
        models = m.classification(refined,multioutput)
        return models
    
   
    def recommend(self,models,data,factors):
        models["model"] = [pickle.loads(x) for x in models["model"]]
        prediction_set = m.predict(models,data,factors)
        return prediction_set