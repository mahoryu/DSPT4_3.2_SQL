# Module_4/Assignment/titanic_queries.py

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


# How many passengers survived, and how many died?
query = """
SELECT 
	survived
	, COUNT(id) as survival_count
FROM titanic
GROUP BY survived
"""

cur.execute(query)
results = cur.fetchall()
print(f"The number of survivors was: {results[1][1]}")
print(f"The number that perished was: {results[0][1]}\n")

# How many passengers were in each class?
query = """
SELECT 
	pclass as class
	, COUNT(id) as number_per_class
FROM titanic
GROUP BY pclass
ORDER BY pclass
"""

cur.execute(query)
results = cur.fetchall()
print(f"The number of first class passengers was: {results[0][1]}")
print(f"The number of second class passengers was: {results[1][1]}")
print(f"The number of third class passengers was: {results[2][1]}\n")

# How many passengers survived/died within each class?
query = """
SELECT 
	pclass as class
	, survived
	, COUNT(id) as survival_per_class
FROM titanic
GROUP BY pclass, survived
ORDER BY pclass, survived
"""

cur.execute(query)
results = cur.fetchall()
print("Survival Rate:")
print(f"First class: {results[1][2]} survived and {results[0][2]} perished")
print(f"Second class: {results[3][2]} survived and {results[2][2]} perished")
print(f"Third class: {results[5][2]} survived and {results[4][2]} perished\n")

# What was the average age of survivors vs nonsurvivors?
query = """
SELECT
	survived
	, ROUND(AVG(age)::NUMERIC) as average_age
FROM titanic
GROUP BY survived
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average age of survivors was: {results[1][1]}")
print(f"The average age of those that perished was: {results[0][1]}\n")

# What was the average age of each passenger class?
query = """
SELECT
	pclass as class
	, ROUND(AVG(age)::NUMERIC) as average_age
FROM titanic
GROUP BY pclass
ORDER BY pclass
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average age of first class was: {results[0][1]}")
print(f"The average age of second class was: {results[1][1]}")
print(f"The average age of thrid class was: {results[2][1]}\n")

# What was the average fare by passenger class? By survival?
query = """
SELECT
	survived
	, ROUND(AVG(fare)::NUMERIC,2) as average_fare
FROM titanic
GROUP BY survived
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average fare of survivors was: ${results[1][1]}")
print(f"The average fare of those that perished was: ${results[0][1]}\n")

# How many siblings/spouses aboard on average, by passenger class? By survival?
query = """
SELECT
	ROUND(AVG(siblings_spouses_aboard)::NUMERIC,3) as avg_num_sib_spouse
FROM titanic
"""
query2 = """
SELECT
	pclass
	, ROUND(AVG(siblings_spouses_aboard)::NUMERIC,3) as avg_num_sib_spouse
FROM titanic
GROUP BY pclass
order BY pclass
"""
query3 = """
SELECT
	survived
	, ROUND(AVG(siblings_spouses_aboard)::NUMERIC,3) as avg_num_sib_spouse
FROM titanic
GROUP BY survived
order BY survived
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average number of siblings/spouses aboard was: {results[0][0]}\n")

cur.execute(query2)
results = cur.fetchall()
print("Average number of siblings/spouses aboard by class:")
print(f"First Class: {results[0][1]}")
print(f"Second Class: {results[1][1]}")
print(f"Third Class: {results[2][1]}\n")

cur.execute(query3)
results = cur.fetchall()
print("Average number of siblings/spouses aboard by survival:")
print(f"Survived: {results[1][1]}")
print(f"Perished: {results[0][1]}\n")

# How many parents/children aboard on average, by passenger class? By survival?
query = """
SELECT
	ROUND(AVG(parents_children_aboard)::NUMERIC,3) as avg_num_sib_spouse
FROM titanic
"""
query2 = """
SELECT
	pclass
	, ROUND(AVG(parents_children_aboard)::NUMERIC,3) as avg_num_sib_spouse
FROM titanic
GROUP BY pclass
order BY pclass
"""
query3 = """
SELECT
	survived
	, ROUND(AVG(parents_children_aboard)::NUMERIC,3) as avg_num_sib_spouse
FROM titanic
GROUP BY survived
order BY survived
"""

cur.execute(query)
results = cur.fetchall()
print(f"The average number of parents/children aboard was: {results[0][0]}\n")

cur.execute(query2)
results = cur.fetchall()
print("Average number of parents/children aboard by class:")
print(f"First Class: {results[0][1]}")
print(f"Second Class: {results[1][1]}")
print(f"Third Class: {results[2][1]}\n")

cur.execute(query3)
results = cur.fetchall()
print("Average number of parents/children aboard by survival:")
print(f"Survived: {results[1][1]}")
print(f"Perished: {results[0][1]}\n")

# Do any passengers have the same name?
query = """
SELECT
	*
	, (total_passengers - distinct_names) as people_with_same_name
FROM
	(SELECT
		COUNT(DISTINCT id) as total_passengers
		, COUNT(DISTINCT "name") as distinct_names
	FROM titanic) as subquery
"""

cur.execute(query)
results = cur.fetchall()
print(f"There are {results[0][2]} passengers with the same name.\n")

# Do any passengers have the same first name?
query = """
SELECT
	SUM(num_occurance)
FROM
	(SELECT
		COUNT(first_name) num_occurance
		, first_name
	FROM 
		(SELECT
			SUBSTRING("name" FROM '\ [A-Za-z]+') first_name
			, "name" full_name
		FROM titanic) as subsubquery

	GROUP BY first_name
	ORDER BY num_occurance DESC) as sub
WHERE num_occurance > 1
"""

cur.execute(query)
results = cur.fetchall()
print(f"There are {results[0][0]} passengers with the same first name.\n")

# How many married couples were aboard the Titanic? Assume that two people (one Mr. and one Mrs.)
#   with the same last name and with at least 1 sibling/spouse aboard are a married couple.
query = """
SELECT
	*
	, possible_couples - non_dups as num_couples
FROM
	(SELECT
		COUNT(SUBSTRING("name" FROM '[A-Z][a-z]+$')) possible_couples
		, COUNT(DISTINCT SUBSTRING("name" FROM '[A-Z][a-z]+$')) non_dups
	FROM titanic
	WHERE substring("name" FROM 'Mrs*') IS NOT NULL AND siblings_spouses_aboard > 0) AS subquery
"""

cur.execute(query)
results = cur.fetchall()
print(f"There are {results[0][2]} couples on board.")