from datetime import datetime

from fund.fund import Fund
from pricer.pricer import Pricer as pricer_list
from ranker.ranker import Ranker as ranker_list
from classifier.classifier import Classifier as classifier_list
from portfolio.aportfolio import APortfolio

parameter = {
    "value":True
    ,"ceiling":True
    ,"classification":False
    ,"rank":False
    ,"short":False
    ,"risk":"rrr"
    ,"market_return":1.15
    ,"buy_day":5
    ,"sell_day":1
    ,"floor_value":1
    ,"tyields":"tyields10"
}

portfolio = APortfolio(pricer_list.DAILY_STOCK_WINDOW
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolio.load_optimal_parameter(parameter)
portfolios = []
portfolios.append(portfolio)

start = datetime(datetime.now().year,1,1)
end = datetime.now()

class MainBabFund(object):
    @classmethod
    def load_fund(self):
        fund = Fund(portfolios,start,end,end)
        fund.initialize_portfolios()
        fund.initialize_backtesters()
        return fund