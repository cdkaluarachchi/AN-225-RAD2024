import os
import urllib.parse as up
import psycopg2

up.uses_netloc.append("postgres")
conn = psycopg2.connect(database="gdnjevix", user="gdnjevix", password="71O_c8x-OaR_yFJNiatPmqh3N0KjjZhm", host="floppy.db.elephantsql.com", port="5432")
cur = conn.cursor()


data = ((12,),(32,),(22,),(72,))
#cur.executemany("INSERT INTO abc (age) VALUES (%s)", data)
""" for item in data:
    cur.execute("INSERT INTO abc (age) VALUES (%s)", item) """
#cur.close
cur.execute("SELECT * FROM abc;")
print(cur.fetchall())