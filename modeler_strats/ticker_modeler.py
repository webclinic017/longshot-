from modeler.modeler import Modeler as m
import pickle
import pandas as pd

class TickerModeler(object):

    def __init__(self):
        self.name = "ticker"

    def model(self,training_set,prediction_set,factors):
        predictions = []
        for ticker in training_set["ticker"].unique():
            ticker_training_set = training_set[training_set["ticker"]==ticker].reset_index()
            refined = {"X":ticker_training_set[factors],"y":ticker_training_set["y"]}
            models = m.regression(refined)
            ticker_prediction_set = m.predict(models,prediction_set[prediction_set["ticker"]==ticker],factors)
            predictions.append(ticker_prediction_set)
        return pd.concat(predictions)
    

    def recommend_model(self,training_set,factors):
        completed_models = []
        for ticker in training_set["ticker"].unique():
            ticker_training_set = training_set[training_set["ticker"]==ticker].reset_index()
            refined = {"X":ticker_training_set[factors],"y":ticker_training_set["y"]}
            models = m.regression(refined)
            models["ticker"] = ticker
            completed_models.append(completed_models)
        return pd.concat(completed_models)
    
 
    def recommend(self,models,recs,data,factors):
        recs = []
        models["model"] = [pickle.loads(x) for x in models["model"]]
        for ticker in models["ticker"].unique():
            ticker_data = data[data["ticker"]==ticker]
            prediction_set = self.strat_class.recommend_set(recs,ticker_data)
            prediction_set = m.predict(models,prediction_set,factors)
            recs.append(prediction_set)
        return pd.concat(recs)