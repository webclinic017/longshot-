from matplotlib import ticker
from modeler.modeler import Modeler as m
import pickle
import pandas as pd

class IndustryModeler(object):

    def __init__(self):
        self.name = "industry"

    def model(self,training_set,prediction_set,factors):
        predictions = []
        for industry in training_set["GICS Sector"].unique():
            industry_training_set = training_set[training_set["GICS Sector"]==industry].reset_index().dropna()
            refined = {"X":industry_training_set[factors],"y":industry_training_set["y"]}
            models = m.regression(refined)
            ticker_prediction_set = prediction_set[prediction_set["GICS Sector"]==industry].dropna()
            ticker_prediction_set = m.predict(models,ticker_prediction_set,factors)
            predictions.append(ticker_prediction_set)
        return pd.concat(predictions)
    
    def recommend_model(self,training_set,factors):
        completed_models = []
        for industry in training_set["GICS Sector"].unique():
            industry_training_set = training_set[training_set["GICS Sector"]==industry].reset_index()
            refined = {"X":industry_training_set[factors],"y":industry_training_set["y"]}
            models = m.regression(refined)
            models["GICS Sector"] = industry
            completed_models.append(models)
        return pd.concat(completed_models)
    
    def recommend(self,models,data,factors):
        recs = []
        models["model"] = [pickle.loads(x) for x in models["model"]]
        for industry in models["GICS Sector"].unique():
            industry_data = data[data["GICS Sector"]==industry]
            ticker_models = models[models["GICS Sector"]==industry]
            prediction_set = m.predict(ticker_models,industry_data,factors)
            recs.append(prediction_set)
        return pd.concat(recs)