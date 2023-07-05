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
from processor.processor import Processor as p

market = Market()
start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
end = datetime.now().strftime("%Y-%m-%d")

portfolio = APortfolio(pricer_list.WEEKLY_STOCK_ROLLING
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolio_ii = APortfolio(pricer_list.WEEKLY_CRYPTO_ROLLING
                          ,classifier_list.NONE
                          ,ranker_list.NONE)

portfolios = []
portfolios.append(portfolio)
portfolios.append(portfolio_ii)
fund = Fund(portfolios,start,end,end)

fund.initialize_portfolios()
modeler_strat = UniversalModeler()

hour_to_deploy = 11
new_york_date = datetime.now(tz=timezone(offset=timedelta(hours=-4)))
year = new_york_date.year
week = new_york_date.isocalendar()[1]

market.cloud_connect()
sp500 = market.retrieve("sp500").rename(columns={"Symbol":"ticker"})
market.disconnect()

for portfolio in fund.portfolios:
    try:
        pricer_class = portfolio.pricer_class
        tickers = sp500["ticker"].unique() if pricer_class.asset_class.value == "stocks" else ["BTC"]
        training_sets = []
        
        market.cloud_connect()
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
        if pricer_class.isai:
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
        else:
            pricer_class.db.cloud_connect()
            pricer_class.db.drop("predictions")
            prediction_set = training_data[training_data["year"]==year].reset_index(drop=True)
            stuff = pricer_class.sim_processor(prediction_set)
            relevant_columns = list(set(list(stuff.columns)) - set(pricer_class.factors))
            pricer_class.db.store("predictions",stuff[relevant_columns])
            pricer_class.db.disconnect()

    except Exception as e:
            portfolio.db.cloud_connect()
            portfolio.db.store("errors",pd.DataFrame([{"date":str(datetime.now()),"status":"predictions","error":str(e)}]))
            portfolio.db.disconnect()
