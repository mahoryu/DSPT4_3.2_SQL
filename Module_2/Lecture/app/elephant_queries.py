# app/elephant_queries.py

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD =os.getenv("DB_PASSWORD")

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
print(type(conn)) #> <class 'psycopg2.extensions.connection'>

cur = conn.cursor()
print(type(cur)) #> <class 'psycopg2.extensions.cursor'>

query = "SELECT * from test_table;"

#cur.execute(query)
#results = cur.fetchone()
#print(type(results))
#print(results)

cur.execute(query)
results = cur.fetchall()
print(type(results)) #> list
print(results)
