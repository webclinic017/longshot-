from pymongo import MongoClient, DESCENDING
import pandas as pd
from database.idatabase import IDatabase
import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
token = os.getenv("MONGO_KEY")
import certifi
ca = certifi.where()

## Database abstract class, most methods are core to database functions
class ADatabase(IDatabase):
    
    def __init__(self,name):
        self.name = name
        super().__init__()
    
    ## returns all table names
    def collection_names(self):
        return self.client[self.name].collection_names()
    
    ## connects to the db
    def connect(self):
        self.client = MongoClient("localhost",27017)
    
    ## connects to the cloud version of the db
    def cloud_connect(self):
        self.client = MongoClient(token,tlsCAFile=ca)
    
    ## disconnects to the db, connect and disconnect should be paired to avoid connection errors
    def disconnect(self):
        self.client.close()

    ## storing data 
    def store(self,table_name,data):
        try:
            db = self.client[self.name]
            table = db[table_name]
            records = data.to_dict("records")
            table.insert_many(records)
        except Exception as e:
            print(self.name,table_name,str(e))
    ## retrieving data
    def retrieve(self,table_name):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.find({},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,table_name,str(e))
    
    ## retrieve based on a query rarely used
    def query(self,table_name,query):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.find(query,{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,table_name,str(e))
    
    ## update a value not used much
    def update(self,table_name,query,update):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.find_one_and_update(query,{"$set":query})
        except Exception as e:
            print(self.name,table_name,str(e))

    ## deletes a value 
    def delete(self,table_name,query):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.delete_many(query)
        except Exception as e:
            print(self.name,table_name,str(e))
    
    ## drops a table
    def drop(self,table_name):
        try:
            db = self.client[self.name]
            table = db[table_name]
            table.drop()
        except Exception as e:
            print(self.name,table_name,str(e))
    
    ## for time series data retrieves all dates in the series
    def retrieve_collection_date_range(self,collection_name):
        try:
            db = self.client[self.name]
            table = db[collection_name]
            data = table.find({},{"date":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
