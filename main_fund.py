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
    ,"risk":"none"
    ,"market_return":1.15
    ,"buy_day":1
    ,"sell_day":15
    ,"floor_value":1
    ,"tyields":"tyields"
}

btc_parameter = {
    "value":True
    ,"ceiling":True
    ,"classification":False
    ,"rank":False
    ,"short":False
    ,"risk":"none"
    ,"market_return":1.15
    ,"buy_day":1
    ,"sell_day":5
    ,"floor_value":1
    ,"tyields":"tyields10"
}

portfolio = APortfolio(pricer_list.WEEKLY_STOCK_ROLLING
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolio_ii = APortfolio(pricer_list.WEEKLY_CRYPTO_WINDOW
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolio.load_optimal_parameter(parameter)
portfolio_ii.load_optimal_parameter(btc_parameter)

portfolios = []
portfolios.append(portfolio_ii)
portfolios.append(portfolio)

start = datetime(datetime.now().year,1,1)
end = datetime.now()

class MainFund(object):
    @classmethod
    def load_fund(self):
        fund = Fund(portfolios,start,end,end)
        fund.initialize_portfolios()
        fund.initialize_backtesters()
        return fund