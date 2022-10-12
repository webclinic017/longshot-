class QueueFactory(object):

    @classmethod
    def queue_record(self,position_dictionary,offering,swap):
        queue_dictionary = {}
        queue_dictionary["buy_price"] = offering["adjclose"]
        queue_dictionary["adjclose"] = offering["adjclose"]
        queue_dictionary["ticker"] = offering["ticker"]
        queue_dictionary["amount"] = int(position_dictionary["cash"] / offering["adjclose"])
        queue_dictionary["pv"] = queue_dictionary["amount"] * offering["adjclose"]
        queue_dictionary["projected_delta"] = offering["delta"]
        queue_dictionary["swap"] = swap
        return queue_dictionary
