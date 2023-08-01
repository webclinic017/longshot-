import pandas as pd

class SellFactory(object):

    @classmethod
    def sell_record(self,trade,date,position,parameter):
        new_dictionary = {}
        for key in trade.keys():
            new_dictionary[key] = trade[key]
        current_price = trade["adjclose"]
        trade["sell_date"] = date
        trade["sell_price"] = min(trade["buy_price"]*(1+parameter["req"]),current_price)
        trade["delta"] = (trade["sell_price"] - trade["buy_price"]) / trade["buy_price"]
        trade["position"] = position
        for key in parameter.keys():
            trade[key] = parameter[key]
        return trade
