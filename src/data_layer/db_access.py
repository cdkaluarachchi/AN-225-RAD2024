import os
import urllib.parse as up
import psycopg2

up.uses_netloc.append("postgres")

conn = psycopg2.connect(database="gdnjevix",
                            user="gdnjevix",
                            password="71O_c8x-OaR_yFJNiatPmqh3N0KjjZhm",
                            host="floppy.db.elephantsql.com",
                            port="5432")
cur = conn.cursor()


# data = ((12,),(32,),(22,),(72,))
# #cur.executemany("INSERT INTO abc (age) VALUES (%s)", data)
# """ for item in data:
#     cur.execute("INSERT INTO abc (age) VALUES (%s)", item) """
# #cur.close
# cur.execute("SELECT * FROM abc;")
# print(cur.fetchall())
data = [(12,),(32,),(22,),(72,)]
api_data = '{"coord": {"lon": 79.8612, "lat": 6.9271}, "weather": [{"id": 803, "main": "Clouds", "description": "broken clouds", "icon": "04n"}], "base": "stations", "main": {"temp": 299.43, "feels_like": 299.43, "temp_min": 299.43, "temp_max": 299.43, "pressure": 1013, "humidity": 77, "sea_level": 1013, "grnd_level": 1012}, "visibility": 10000, "wind": {"speed": 1.39, "deg": 172, "gust": 1.69}, "clouds": {"all": 59}, "dt": 1710440641, "sys": {"country": "LK", "sunrise": 1710377258, "sunset": 1710420717}, "timezone": 19800, "id": 1248991, "name": "Colombo", "cod": 200}'



cur.execute(sql, api_data)
conn.commit()

conn.close()

#end