"""
Example of upserting dataframe
"""
import pandas as pd
from src.mongodb_helper import MongoDBHelper

uri = "mongodb://localhost:27017/"
helper = MongoDBHelper(uri)
database = "test_database"
collection = "test_collection"

data = pd.DataFrame({'_id': [1, 2, 3],
                     'first_name': ['John', 'John', 'Ivan'],
                     'last_name': ['Li', 'Wong', 'Chong'],
                     'age': [20, 25, 24],
                     })

helper.upsert_df(df=data, database_name=database, collection_name=collection, primary_key_column='_id')
output = helper.get_df(database_name=database, collection_name=collection)
print(output.head())

# Dataframe changes
data_v2 = pd.DataFrame({'_id': [1, 2, 3],
                        'first_name': ['John', 'John', 'Ivan'],
                        'last_name': ['Li', 'Lam', 'Chong'],
                        'age': [20, 76, 54],
                        })
helper.upsert_df(df=data_v2, database_name=database, collection_name=collection, primary_key_column='_id')
output = helper.get_df(database_name=database, collection_name=collection)
print(output.head())
