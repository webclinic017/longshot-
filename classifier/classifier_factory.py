from classifier.classifier import Classifier as classifier_list
from classifier.speculation_classifier import SpeculationClassifier
from asset_classes.asset_classes import AssetClasses
from time_horizons.time_horizons import TimeHorizons
class ClassifierFactory(object):

    @classmethod
    def build(self,classifier):
        match classifier:
            case classifier_list.NONE:
                result = None
            case classifier_list.YEARLY_STOCK_ROLLING_CLASSIFIER:
                result =  SpeculationClassifier(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case _:
                result = None
        return result