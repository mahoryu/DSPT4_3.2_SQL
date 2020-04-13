# app/chinook_queries.py

import os
import sqlite3

#DB_FILEPATH = "data/chinook.db"
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "data", "chinook.db")

conn = sqlite3.connect(DB_FILEPATH)
conn.row_factory = sqlite3.Row
print(type(conn)) #> <class 'sqlite3.Connection'>
curs = conn.cursor()
print(type(curs)) #> <class 'sqlite3.Cursor'>
query = "SELECT * FROM customers LIMIT 3"
results2 = curs.execute(query).fetchall()
print("--------")
print("RESULTS 2", results2)
print("----------")
results3 = curs.execute(query).fetchone()
print("RESULTS 3", results3)
print(results3["FirstName"])
#breakpoint()