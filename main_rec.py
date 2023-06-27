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

deploy = True

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

day_to_deploy = parameter["buy_day"]
hour_to_deploy = 11
new_york_date = datetime.now(tz=timezone(offset=timedelta(hours=-4)))
year = new_york_date.year
week = new_york_date.isocalendar()[1]

market.cloud_connect()
sp500 = market.retrieve("sp500").rename(columns={"Symbol":"ticker"})
tyields = Products.tyields(market.retrieve("tyields"))
bench = Products.spy_bench(market.retrieve("spy"))
for portfolio in fund.portfolios:
    try:
        pricer_class = portfolio.pricer_class
        tickers = sp500["ticker"].unique() if pricer_class.asset_class.value == "stocks" else ["BTC"]
        positions = 20 if pricer_class.asset_class.value == "stocks" else 1
        training_sets = []
        for ticker in tickers:
            try:
                prices = market.retrieve_ticker_prices(pricer_class.asset_class.value,ticker)
                prices = p.column_date_processing(prices)
                ticker_data = pricer_class.training_set(ticker,prices,True)
                training_sets.append(ticker_data)
            except Exception as e:
                print(ticker,str(e))
                continue
        data = pd.concat(training_sets)
        training_data = data.dropna().copy().sort_values(["year","week"])
        market.disconnect()
        # making predictions
        pricer_class.db.cloud_connect()
        pricer_class.db.drop("predictions")
        prediction_set = training_data[training_data["year"]==year].reset_index(drop=True)
        models = pricer_class.db.retrieve("models")
        stuff = modeler_strat.recommend(models,prediction_set,pricer_class.factors)
        stuff = stuff.rename(columns={"prediction":f"price_prediction"})
        stuff = pricer_class.sim_processor(stuff)
        relevant_columns = list(set(list(stuff.columns)) - set(pricer_class.factors))
        pricer_class.db.store("predictions",stuff[relevant_columns])
        pricer_class.db.disconnect()

        # making recs
        market.cloud_connect()
        returns = portfolio.create_returns(market,bench,True)
        market.disconnect()

        sim = stuff[relevant_columns].copy().drop("adjclose",axis=1)
        merged = portfolio.merge_sim_returns(sim,returns)
        merged = merged.sort_values(["year","week","day"]).dropna()
        stuff = portfolio.backtester.recommendation(merged.copy(),parameter,tyields)

        rec = stuff[0]
        rec_filtered = rec[(rec["week"]==week)]
        trades = rec_filtered.head(positions).merge(sp500[["ticker","Security","GICS Sector"]],on="ticker")
        final = trades[["year","week","ticker","Security","GICS Sector","weekly_delta","weekly_delta_sign"]]

        if final.index.size > 0:
            portfolio.db.cloud_connect()
            portfolio.db.store("recs",final)
            portfolio.db.disconnect()

    except Exception as e:
            portfolio.db.cloud_connect()
            portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"error":str(e)}]))
            portfolio.db.disconnect()
