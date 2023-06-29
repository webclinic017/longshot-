from datetime import datetime, timedelta

from database.market import Market
from datetime import datetime
from fund.fund import Fund

from pricer.pricer import Pricer as pricer_list
from ranker.ranker import Ranker as ranker_list
from classifier.classifier import Classifier as classifier_list
from portfolio.aportfolio import APortfolio

market = Market()

parameter = {
    "value":True
    ,"ceiling":True
    ,"classification":False
    ,"rank":False
    ,"short":False
    ,"risk":True
    ,"market_return":1.15
    ,"buy_day":1
    ,"sell_day":5
}

start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
end = datetime.now().strftime("%Y-%m-%d")

portfolio = APortfolio(pricer_list.WEEKLY_STOCK_SPECULATION
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolio_ii = APortfolio(pricer_list.WEEKLY_CRYPTO_SPECULATION
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolios = [portfolio,portfolio_ii]

fund = Fund(portfolios,start,end,end)
fund.initialize_portfolios()
fund.initialize_backtesters()

fund.run_recommendation(parameter)