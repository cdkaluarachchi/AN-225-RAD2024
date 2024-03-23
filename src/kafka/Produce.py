from confluent_kafka import Producer
import requests
import time
import json

API_KEY = "5e68e78603755969e4dbb47f458154bf"

url = f"https://api.openweathermap.org/data/2.5/weather?lat=6.9271&lon=79.8612&appid={API_KEY}"

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)
topic = 'quickstart-events'

try:
    while True:
        data = requests.get(url)
        parsed = data.json()
        producer.produce(topic, value=json.dumps(parsed).encode('utf-8'))
        producer.flush() 
        time.sleep(300)

except KeyboardInterrupt:
    pass

finally:
    producer.flush() 