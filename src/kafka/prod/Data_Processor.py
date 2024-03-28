#!/usr/bin/python3
from confluent_kafka import Producer, Consumer, KafkaError
import requests
import time
import json

import urllib.parse as up
import psycopg2
import datetime
import pytz
import os

os.chdir('/root/Weather_Data_Stream/')

API_KEY = "5e68e78603755969e4dbb47f458154bf"
TOPIC = 'weatherdata'
METADATALOC = f"./Metadata/locationdata.json"

def load_json(file_path):
        # Read JSON data from the file
    try:
        with open(file_path, 'r') as file:
            json_data = file.read()
        # Load JSON into a dictionary
        data_dict = json.loads(json_data)
        return data_dict
    except Exception as e:
        print(f"An error occurred: {e}")

data = load_json(METADATALOC)

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

# Request data from API for each location and send to kafka
execute_timestamp = datetime.datetime.now(pytz.utc)
ist_timezone = pytz.timezone('Asia/Kolkata')
pone_ist_execute_timestamp = execute_timestamp.astimezone(ist_timezone)

try:
    for location in data:
        wdata = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={data[location][0]}&lon={data[location][1]}&appid={API_KEY}").json()
        producer.produce(TOPIC, value=json.dumps(wdata).encode('utf-8'))
        producer.flush()
        print(f"Data Fetched for {location}...")
        time.sleep(1)
except Exception as e:
    print(f"Error in Phase 1 {e}") 

print(f"Phase 1 Complete {pone_ist_execute_timestamp}")


# Consume from Kafka and send data to DB

SOURCE = "www.opeanweather.com"

up.uses_netloc.append("postgres")

try:

    conn = psycopg2.connect(database="gdnjevix",
                                user="gdnjevix",
                                password="71O_c8x-OaR_yFJNiatPmqh3N0KjjZhm",
                                host="floppy.db.elephantsql.com",
                                port="5432")
    cur = conn.cursor()
    
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    
    ptwo_ist_execute_timestamp = execute_timestamp.astimezone(ist_timezone)

    while True:
        msg = consumer.poll(1.0) 

        if msg is None:
            print("No data in queue")
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Error in phase 2 {msg.error().code()}")
                break
            else:
                print(f"Error in phase 2 {msg.error()}")
                break

        decoded_data = msg.value().decode('utf-8')
        datadict = eval(decoded_data)
        ptwo_ist_execute_timestamp = execute_timestamp.astimezone(ist_timezone)

        #Landing zone data
        cur.execute(f'INSERT into LZ_API_DATA(api_data, data_source, sys_insert_datetime) VALUES(\'{decoded_data}\', \'{SOURCE}\',\'{ptwo_ist_execute_timestamp}\') RETURNING id')
        conn.commit() 
        api_id = cur.fetchone()[0]

        #weather 
        weather = datadict['weather'][0]

        cur.execute(f'CALL SP_DIM_WEATHER_INSERT_V15(\'{int(weather["id"])}\', \'{weather["main"]}\', \'{weather["description"]}\', \'{weather["icon"]}\', \'{int(api_id)}\', \'{ptwo_ist_execute_timestamp}\')')
        conn.commit()

        #location
        sys = datadict['sys'] #dict
        timezone = datadict['timezone']
        id = datadict['id']
        loc_name = datadict['name']
        coord = datadict['coord'] #dict

        cur.execute(f'CALL SP_DIM_LOCATION_INSERT_V17(\'{sys["country"]}\',\'{int(timezone)}\', \'{int(id)}\', \'{loc_name}\', \'{coord["lon"]}\', \'{coord["lat"]}\', \'{api_id}\', \'{ptwo_ist_execute_timestamp}\')')
        conn.commit()

        #fact main
        main = datadict['main'] #dict
        wind = datadict['wind'] #dict

        cur.execute(f'CALL FACT_MAIN_INSERT_V2(\'{id}\', \'{weather["id"]}\', \'{main["temp"]}\', \'{main["feels_like"]}\', \'{main["temp_min"]}\', \'{main["temp_max"]}\', \'{main["pressure"]}\', \'{main["humidity"]}\', \'{main["sea_level"]}\', \'{main["grnd_level"]}\', \'{wind["speed"]}\', \'{wind["deg"]}\', \'{wind["gust"]}\', \'{api_id}\', \'{ptwo_ist_execute_timestamp}\')')
        conn.commit()

        print(f'Inserted Record to DB {execute_timestamp}')

    conn.close()

except Exception as e:
    print(f"Error in Phase 2 {e}")

finally:
    print(f"Phase 2 Complete {ptwo_ist_execute_timestamp}")