import pandas as pd
from classifier.classifier import Classifier as classifier_list
from time_horizons.time_horizons import TimeHorizons as timehorizons

class ClassifierFactory(object):

    @classmethod
    def build(self,classifier):
        match classifier:
            case classifier_list.NONE:
                result = None 
            case _:
                result = None
        return result