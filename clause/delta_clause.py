
class DeltaClause(object):
    
    def __init__(self):
        self.name = "delta_clause"

    @classmethod
    def exit_clause(self,position_dictionary,req):
        if len(position_dictionary["asset"].keys()) > 0:
            asset_dictionary = position_dictionary["asset"]
            current_price = asset_dictionary["adjclose"]
            buy_price = asset_dictionary["buy_price"]
            current_gain = (current_price - buy_price) / buy_price
            return current_gain >= req
        else:
            return False
    
    @classmethod
    def swap_clause(self,offerings,position_dictionary,signal,position):
        asset_dictionary = position_dictionary["asset"]
        if len(asset_dictionary.keys()) > 0 and offerings.index.size > position:
            offering = offerings.iloc[position]
            current_price = asset_dictionary["adjclose"]
            buy_price = asset_dictionary["buy_price"]
            current_gain = (current_price - buy_price) / buy_price
            return (current_gain > 0) and (offering["delta"] > (asset_dictionary["projected_delta"] - current_gain)) and (offering["delta"] >= signal)
        else:
            return False
    
    @classmethod
    def queue_clause(self,position_dictionary,todays_recs):
        return (len(position_dictionary["asset"].keys()) == 0) and (todays_recs.index.size > 0)
    
    @classmethod
    def entry_clause(self,date,position_dictionary,prices):
        if len(position_dictionary["queue"].keys())>0 and len(position_dictionary["asset"].keys()) == 0:
            purchase_dictionary = position_dictionary["queue"]
            entry_price = purchase_dictionary["adjclose"]
            ticker = purchase_dictionary["ticker"]
            price_data = prices[(prices["ticker"]==ticker) & (prices["date"]==date)]
            if price_data.index.size > 0:
                high = price_data["high"].iloc[0].item()
                low = price_data["low"].iloc[0].item()
                return entry_price <= high and entry_price >= low
        return False