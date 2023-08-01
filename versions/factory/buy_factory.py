import pandas as pd


class BuyFactory(object):

    @classmethod
    def buy_record(self,date,queue_dictionary):
        purchase_dictionary = queue_dictionary
        purchase_dictionary["date"] = date
        return purchase_dictionary
    
