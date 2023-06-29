from datetime import datetime, timezone, timedelta

from database.market import Market
from processor.processor import Processor as p
from modeler_strats.universal_modeler import UniversalModeler
from datetime import datetime
from fund.fund import Fund

from pricer.pricer import Pricer as pricer_list
from ranker.ranker import Ranker as ranker_list
from classifier.classifier import Classifier as classifier_list
from portfolio.aportfolio import APortfolio
import pandas as pd

from returns.products import Products
from processor.processor import Processor as p
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
modeler_strat = UniversalModeler()

new_york_date = datetime.now(tz=timezone(offset=timedelta(hours=-4)))
week = new_york_date.isocalendar()[1]

market.cloud_connect()
sp500 = market.retrieve("sp500").rename(columns={"Symbol":"ticker"})
tyields = Products.tyields(market.retrieve("tyields"))
bench = Products.spy_bench(market.retrieve("spy"))
market.disconnect()
for portfolio in fund.portfolios:
    try:
        pricer_class = portfolio.pricer_class

        # making returns
        market.cloud_connect()
        returns = portfolio.create_returns(market,bench,True)
        market.disconnect()

        # pulling predictions
        sim = portfolio.create_current_simulation()

        # recommendations
        merged = portfolio.merge_sim_returns(sim,returns)
        merged = merged.sort_values(["year","week","day"]).dropna()
        rec = portfolio.backtester.recommendation(merged.copy(),parameter,tyields)
        rec_filtered = rec[(rec["week"]==week)].sort_values("weekly_delta",ascending=False)
        trades = rec_filtered.merge(sp500[["ticker","Security","GICS Sector"]],on="ticker")
        final = trades[["year","week","ticker","Security","GICS Sector","weekly_delta","weekly_delta_sign"]]

        if final.index.size > 0:
            portfolio.db.cloud_connect()
            portfolio.db.store("recs",final)
            portfolio.db.disconnect()

    except Exception as e:
            portfolio.db.cloud_connect()
            portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"recommendations","error":str(e)}]))
            portfolio.db.disconnect()
