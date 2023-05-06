from modeler.modeler import Modeler as m
import pickle
import pandas as pd

class SubIndustryModeler(object):

    def __init__(self):
        self.name = "subindustry"

    def model(self,training_set,prediction_set,factors):
        predictions = []
        for sub_industry in training_set["GICS Sub-Industry"].unique():
            try:
                sub_industry_training_set = training_set[training_set["GICS Sub-Industry"]==sub_industry].reset_index()
                refined = {"X":sub_industry_training_set[factors],"y":sub_industry_training_set["y"]}
                models = m.regression(refined)
                ticker_prediction_set = prediction_set[prediction_set["GICS Sub-Industry"]==sub_industry].dropna()
                ticker_prediction_set = m.predict(models,ticker_prediction_set,factors)
                predictions.append(ticker_prediction_set)
            except:
                continue
        return pd.concat(predictions)
    
    
    def recommend_model(self,training_set,factors):
        completed_models = []
        for sub_industry in training_set["GICS Sub-Industry"].unique():
            sub_industry_training_set = training_set[training_set["GICS Sub-Industry"]==sub_industry].reset_index()
            refined = {"X":sub_industry_training_set[factors],"y":sub_industry_training_set["y"]}
            models = m.regression(refined)
            models["GICS Sub-Industry"] = sub_industry
            completed_models.append(models)
        return pd.concat(completed_models)
    
    
    def recommend(self,models,data,factors):
        recs = []
        models["model"] = [pickle.loads(x) for x in models["model"]]
        for sub_industry in models["GICS Sub-Industry"].unique():
            sub_industry_data = data[data["GICS Sub-Industry"]==sub_industry]
            prediction_set = self.strat_class.recommend_set(recs,sub_industry_data)
            prediction_set = m.predict(models,prediction_set,factors)
            recs.append(prediction_set)
        return pd.concat(recs)