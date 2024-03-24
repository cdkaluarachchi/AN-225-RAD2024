#!/usr/bin/python3
from confluent_kafka import Producer
import requests
import time
import json

API_KEY = "5e68e78603755969e4dbb47f458154bf"

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

data = load_json("../metadata/locationdata.json")

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)
topic = 'quickstart-events'

try:
    #while True:
    for location in data:
        time.sleep(1)
        wdata = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={data[location][0]}&lon={data[location][1]}&appid={API_KEY}").json()
        producer.produce(topic, value=json.dumps(wdata).encode('utf-8'))
        producer.flush()
        #print(wdata)
         #time.sleep(30)
except KeyboardInterrupt:
    pass

finally:
    producer.flush()