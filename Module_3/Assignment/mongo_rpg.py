# Module_3/Assignment/mongo_rpg.py

import pymongo
import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd

load_dotenv()

# Load the mongo database
DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

# Connect the mongo client
connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
mongo_client = pymongo.MongoClient(connection_uri)


# Load the sqlite database
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "..", "Module_1", "Assignment", "data", "rpg_db.sqlite3")

# Connect the sqlite cursor
sqlite_connection = sqlite3.connect(DB_FILEPATH)
sqlite_cursor = sqlite_connection.cursor()

# Get the data from the sqlite database
df = pd.read_sql_query("SELECT * FROM charactercreator_character;", sqlite_connection)

# Convert to dictionary
records = df.to_dict("records")

# Create rpg_table collection
db = mongo_client.inclass_db
collection = db.rpg_table

# print the number of documents in the currect colletion
print("DOCS:", collection.count_documents({}))

# insert the df into the collection
collection.insert_many(records)

# print all the collection names
print(db.list_collection_names())

# print the updated number of documents in the currect colletion
print("DOCS:", collection.count_documents({}))


