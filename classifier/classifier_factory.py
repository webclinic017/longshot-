from classifier.classifier import Classifier as classifier_list
from classifier.speculation_classifier import SpeculationClassifier
from asset_classes.asset_classes import AssetClasses
from time_horizons.time_horizons import TimeHorizons
from classifier.financial_classifier import FinancialClassifier
class ClassifierFactory(object):

    @classmethod
    def build(self,classifier):
        match classifier:
            case classifier_list.NONE:
                result = None
            case classifier_list.WEEKLY_STOCK_SPECULATION_CLASSIFIER:
                result =  SpeculationClassifier(AssetClasses.STOCKS,TimeHorizons.WEEKLY)
            case classifier_list.WEEKLY_CRYPTO_SPECULATION_CLASSIFIER:
                result =  SpeculationClassifier(AssetClasses.CRYPTO,TimeHorizons.WEEKLY)
            case classifier_list.QUARTERLY_STOCK_FINANCIAL_CLASSIFIER:
                result = FinancialClassifier(AssetClasses.STOCKS,TimeHorizons.QUARTERLY)
            case _:
                result = None
        return result