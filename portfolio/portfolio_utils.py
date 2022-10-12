
import queue


class PortfolioUtils(object):
    
    @classmethod
    def asset_updates(self,portfolio,prices):
        positions = len(portfolio["positions"].keys())
        for position in range(positions):
            asset_dictionary = portfolio["positions"][position]["asset"]
            if len(asset_dictionary.keys()) > 0:
                ticker = asset_dictionary["ticker"]
                price_data = prices[(prices["ticker"]==ticker)]
                if price_data.index.size < 1:
                    asset_dictionary["adjclose"] = asset_dictionary["adjclose"]
                else:
                    current_price = prices[(prices["ticker"]==ticker)].iloc[0]["adjclose"].item()
                    asset_dictionary["adjclose"] = current_price
                asset_dictionary["pv"] = asset_dictionary["adjclose"] * asset_dictionary["amount"]
                portfolio["positions"][position]["asset"] = asset_dictionary
            else:
                continue
        return portfolio

    @classmethod
    def current_ticker_list(self,portfolio):
        positions = len(portfolio["positions"].keys())
        current_tickers = [portfolio["positions"][position]["asset"]["ticker"] for position in range(positions)
                                if len(portfolio["positions"][position]["asset"].keys()) > 0]
        queued_tickers = [portfolio["positions"][position]["queue"]["ticker"] for position in range(positions)
                                if len(portfolio["positions"][position]["queue"].keys()) > 1]
        current_tickers.extend(queued_tickers)
        return current_tickers
    
    
    @classmethod
    def portfolio_init(self,parameters,initial,start_date,positions):
        portfolio = {"date":start_date}
        portfolio["positions"] = {}
        for key in parameters.keys():
            portfolio[key] = parameters[key]
        for position in range(positions):
            portfolio["positions"][position] = {"cash":initial/positions,"asset":{},"queue":{}}
        return portfolio
        
    @classmethod
    def exit(self,position_dictionary):
        asset_dictionary = position_dictionary["asset"]
        position_dictionary["cash"] = position_dictionary["cash"] + asset_dictionary["pv"]
        position_dictionary["asset"] = {}
        return position_dictionary

    @classmethod
    def entry(self,position_dictionary,purchase_dictionary):
        position_dictionary["cash"] = position_dictionary["cash"] - purchase_dictionary["pv"]
        position_dictionary["asset"] = purchase_dictionary
        position_dictionary["queue"] = {}
        return position_dictionary