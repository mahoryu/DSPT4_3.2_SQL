# Module_2/Assignment/app/insert_titanic.py

import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

load_dotenv()

DB_HOST = os.getenv("DB_HOST", default="OOPS")
DB_NAME = os.getenv("DB_NAME", default="OOPS")
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")

connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
print("CONNECTION", type(connection))

cursor = connection.cursor()
print("CURSOR", type(cursor))

print("-------------------")
query = "SELECT usename, usecreatedb, usesuper, passwd FROM pg_user;"
print("SQL:", query)
cursor.execute(query)
for row in cursor.fetchall()[0:10]:
    print(row)


#
# CREATE THE TABLE
#

table_name = "titanic"

print("-------------------")
query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  id SERIAL PRIMARY KEY,
  Survived integer,
  Pclass integer,
  Name varchar NOT NULL,
  Sex varchar NOT NULL,
  Age float,
  Siblings_Spouses_Aboard integer,
  Parents_Children_Aboard integer,
  Fare float
);
"""
print("SQL:", query)
cursor.execute(query)

#
# INSERT SOME DATA
#

HTML = "https://raw.githubusercontent.com/mahoryu/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv"
df = pd.read_csv(HTML)

insertion_query = f"INSERT INTO {table_name} (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare) VALUES %s"

records = df.to_dict("records")
list_of_tuples = [(r['Survived'], r['Pclass'], r['Name'], r['Sex'], r['Age'], r['Siblings/Spouses Aboard'], r['Parents/Children Aboard'], r['Fare']) for r in records]

execute_values(cursor, insertion_query, list_of_tuples)


#
# QUERY THE TABLE
#

print("-------------------")
query = f"SELECT * FROM {table_name} LIMIT 5;"
print("SQL:", query)
cursor.execute(query)
for row in cursor.fetchall():
    print(row)

# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()
# Clean up
cursor.close()
connection.close()