import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD =os.getenv("DB_PASSWORD")

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
cur = conn.cursor()


# What was the average fare of the survivors?
query = """
SELECT 
	ROUND(AVG(fare)::NUMERIC,2) as average_fare
	, ROUND(AVG(age)::NUMERIC,2) as average_age
FROM titanic
WHERE survived = 1
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average fare of surviors was: ${results[0][0]}")
print(f"and the avgerage age was: {results[0][1]}\n")


# What was the average fare of those that didn't survive?
query = """
SELECT 
	ROUND(AVG(fare)::NUMERIC,2) as average_fare
	, ROUND(AVG(age)::NUMERIC,2) as average_age
FROM titanic
WHERE survived = 0
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average fare of those that didn't survive was: ${results[0][0]}")
print(f"and the avgerage age was: {results[0][1]}\n")