from math import e
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import r2_score, accuracy_score
from sklearn.linear_model import LinearRegression, SGDRegressor, RidgeCV, SGDClassifier, RidgeClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.svm import SVC
from sklearn.multioutput import MultiOutputClassifier
from sklearn.naive_bayes import GaussianNB
import pandas as pd
import xgboost as xgb
from xgboost.sklearn import XGBClassifier
from xgboost.training import train
from catboost import CatBoostRegressor, CatBoostClassifier
from lightgbm import LGBMRegressor, LGBMClassifier, early_stopping, log_evaluation
import warnings
warnings.filterwarnings(action='ignore')

class Modeler(object):

    @classmethod
    def predict(self,models,prediction_set,factors):
        factors = [x for x in factors if x != "y" and x != "ticker"]
        for row in models.iterrows():
            try:
                model = row[1]["model"]
                api = row[1]["api"]
                score = row[1]["score"]
                prediction_set[f"{api}_prediction"] = model.predict(prediction_set[factors])
                prediction_set[f"{api}_score"] = score 
            except Exception as e:
                print(str(e))
        return prediction_set

    @classmethod
    def classification(self,data,multioutput):
        results = []
        sk_result = self.sk_classify(data,multioutput)
        results.append(sk_result)
        xgb_result = self.xgb_classify(data,multioutput)
        results.append(xgb_result)
        results.append(self.cat_classify(data,multioutput))
        df = pd.DataFrame(results)
        df["model_type"] = "classification"
        return df

    @classmethod
    def regression(self,data):
        results = []
        sk_result = self.sk_regression(data)
        results.append(sk_result)
        xgb_result = self.xgb_regression(data)
        results.append(xgb_result)
        results.append(self.cat_regression(data))
        df = pd.DataFrame(results)
        df["model_type"] = "regression"
        return df
    
    @classmethod
    def xgb_regression(self,data):
        try:
            params = {"booster":["gbtree","gblinear","dart"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            gs = GridSearchCV(xgb.XGBRegressor(objective="reg:squarederror",verbosity = 0),param_grid=params,scoring="r2")
            gs.fit(X_train,y_train)
            predictions = gs.predict(X_test)
            score = r2_score(predictions,y_test)
            model = gs.best_estimator_
            return {"api":"xgb","model":model,"score":score}
        except Exception as e:
            print(str(e))
            return {"api":"xgb","model":str(e),"score":-99999}
    
    @classmethod
    def light_regression(self,data):
        try:
            params = {"boosting_type":[
                                        "gbdt"
                                        ,"goss"
                                        ,"dart"
                                        ,"rf"
                                    ]
                        }
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            gs = GridSearchCV(LGBMRegressor(metric="mape",num_iterations=100,early_stopping_round=3,verbosity = -1,force_col_wise=True),param_grid=params,scoring="r2")
            gs.fit(X_train,y_train)
            predictions = gs.predict(X_test)
            score = r2_score(predictions,y_test)
            model = gs.best_estimator_
            return {"api":"light","model":model,"score":score}
        except Exception as e:
            print(str(e))
            return {"api":"light","model":str(e),"score":-99999}
    
    @classmethod
    def cat_regression(self,data):
        try:
            params = {"boosting_type":[
                                        "Ordered",
                                        "Plain"
                                        ]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            gs = GridSearchCV(CatBoostRegressor(iterations=100,verbose=False,early_stopping_rounds=3),param_grid=params,scoring="r2")
            gs.fit(X_train,y_train)
            predictions = gs.predict(X_test)
            score = r2_score(predictions,y_test)
            model = gs.best_estimator_
            return {"api":"cat","model":model,"score":score}
        except Exception as e:
            print(str(e))
            return {"api":"cat","model":str(e),"score":-99999}
    
    @classmethod
    def sk_regression(self,data):
        stuff = {
            "sgd" : {"model":SGDRegressor(fit_intercept=True),"params":{"loss":["squared_loss","huber"]
                                                            ,"learning_rate":["constant","optimal","adaptive"]
                                                            ,"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
            "r" : {"model":RidgeCV(alphas=[0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1],fit_intercept=True),"params":{}},
            "lr" : {"model":LinearRegression(fit_intercept=True),"params":{"fit_intercept":[True,False]}}
        }
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        results = []
        for regressor in stuff:
            try:
                model = stuff[regressor]["model"]
                model.fit(X_train,y_train)
                y_pred = model.predict(X_test)
                score = r2_score(y_test,y_pred)
                result = {"api":"skl","model":model,"score":score}
                results.append(result)
            except Exception as e:
                print(str(e))
                results.append({"api":"skl","model":str(e),"score":-99999})
        return pd.DataFrame(results).sort_values("score",ascending=False).iloc[0].to_dict()

    @classmethod
    def cat_classify(self,data,multioutput):
        try:
            params = {"boosting_type":["Ordered","Plain"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            if multioutput:
                gs = GridSearchCV(CatBoostClassifier(verbose=False),param_grid=params,scoring="accuracy")
                gs.fit(X_train,y_train)
                model = gs.best_estimator_
            else:
                gs = GridSearchCV(CatBoostClassifier(verbose=False),param_grid=params,scoring="accuracy")
                y_train = LabelEncoder().fit(y_train).transform(y_train)
                gs.fit(X_train,y_train)
                y_test = LabelEncoder().fit(y_test).transform(y_test)
                model = gs.best_estimator_
            score = accuracy_score(model.predict(X_test),y_test)
            return {"api":"cat","model":model,"score":score}
        except Exception as e:
            print(str(e))
            return {"api":"cat","model":str(e),"score":-99999}
    
    @classmethod
    def light_classify(self,data,multioutput):
        try:
            params = {"boosting_type":["gbdt","goss","dart","rf"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            if multioutput:
                models = []
                for booster in params["boosting_type"]:
                    try:
                        gs = MultiOutputClassifier(LGBMClassifier(verbosity = 0,force_col_wise=True,boosting_type=booster))
                        gs.fit(X_train,y_train)
                        model = gs
                        score = accuracy_score(model.predict(X_test),y_test)
                        models.append({"api":"light","model":model,"score":score})
                    except Exception as e:
                        models.append({"api":"light","model":str(e),"score":-99999})
                        continue
                return pd.DataFrame(models).sort_values("score",ascending=False).iloc[0]
            else:
                gs = GridSearchCV(LGBMClassifier(verbosity = 0,force_col_wise=True),param_grid=params,scoring="accuracy")
                y_train = LabelEncoder().fit(y_train).transform(y_train)
                gs.fit(X_train,y_train)
                y_test = LabelEncoder().fit(y_test).transform(y_test)
                model = gs.best_estimator_
            score = accuracy_score(model.predict(X_test),y_test)
            return {"api":"light","model":model,"score":score}
        except Exception as e:
            print(str(e))
            return {"api":"light","model":str(e),"score":-99999}
    
    @classmethod
    def xgb_classify(self,data,multioutput):
        try:
            params = {"booster":["gbtree","gblinear","dart"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            if multioutput:
                models = []
                for booster in params["booster"]:
                    try:
                        gs = MultiOutputClassifier(XGBClassifier(eval_metric="logloss",booster=booster))
                        gs.fit(X_train,y_train)
                        model = gs
                        score = accuracy_score(model.predict(X_test),y_test)
                        models.append({"api":"xgb","model":model,"score":score})
                    except Exception as e:
                        models.append({"api":"xgb","model":str(e),"score":-99999})
                        continue
                return pd.DataFrame(models).sort_values("score",ascending=False).iloc[0]
            else:
                gs = GridSearchCV(xgb.XGBClassifier(objective="binary:logistic",eval_metric="logloss"),param_grid=params,scoring="accuracy")
                y_train = LabelEncoder().fit(y_train).transform(y_train)
                gs.fit(X_train,y_train)
                y_test = LabelEncoder().fit(y_test).transform(y_test)
                model = gs.best_estimator_
            score = accuracy_score(model.predict(X_test),y_test)
            return {"api":"xgb","model":model,"score":score}
        except Exception as e:
            print(str(e))
            return {"api":"xgb","model":str(e),"score":-99999}

    @classmethod
    def sk_classify(self,data,multioutput):
        results = []
        vc = VotingClassifier(estimators=[
                ("ridge", RidgeClassifier()),
                ("tree",DecisionTreeClassifier()),
                ("neighbors",KNeighborsClassifier()),
                ("g",GaussianNB()),
                ("rfc",RandomForestClassifier())])
        stuff = {
                "ridge" : {"model":RidgeClassifier(),"params":{"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
                "tree":{"model":DecisionTreeClassifier(),"params":{'max_depth': range(1,11)}},
                "neighbors":{"model":KNeighborsClassifier(),"params":{'knn__n_neighbors': range(1, 10)}},
                "g":{"model":GaussianNB(),"params":{}},
                "rfc":{"model":RandomForestClassifier(),"params":{"criterion":["gini","entropy"]
                                                                # ,"n_estimators":[100,150,200]
                                                                # ,"max_depth":[None,1,3,5,10]
                                                                # ,"min_samples_split":[5,10]
                                                                # ,"min_samples_leaf":[5,10]
                                                                }},
                "vc":{"model":vc,"params":{}}}
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        results = []
        for classifier in stuff:
            try:
                model = stuff[classifier]["model"]
                params = stuff[classifier]["params"]
                if classifier == "vc" or not multioutput:
                    model.fit(X_train,y_train)
                if multioutput:
                    model = MultiOutputClassifier(model).fit(X_train,y_train)
                else:
                    gs = GridSearchCV(model,params,cv=5,scoring="accuracy")
                    gs.fit(X_train,y_train)
                    model = gs.best_estimator_
                y_pred = model.predict(X_test)
                score = accuracy_score(y_test,y_pred)
                result = {"api":"skl","model":model,"score":score}
                results.append(result)
            except Exception as e:
                results.append({"api":"skl","model":str(e),"score":-99999})
        return pd.DataFrame(results).sort_values("score",ascending=False).iloc[0].to_dict()
    
    @classmethod
    def shuffle_split(self,data):
        X_test = data["X"].iloc[::4]
        X_train = data["X"].drop(index=[x for x in range(0,data["X"].index.size,4)])
        y_test = data["y"].iloc[::4]
        y_train = data["y"].drop(index=[x for x in range(0,data["y"].index.size,4)])
        return [X_train, X_test, y_train, y_test]