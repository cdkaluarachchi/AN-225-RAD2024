from confluent_kafka import Producer
import requests
import time
import json

API_KEY = "5e68e78603755969e4dbb47f458154bf"

url = f"https://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid={API_KEY}"

data = requests.get(url)
parsed = data.json()

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)
topic = 'quickstart-events'


try:
    while True:  # Producing 10 messages as an example

        #message_value = input("Enter Message : ")
        producer.produce(topic, value=json.dumps(parsed).encode('utf-8'))
        producer.flush()  # Ensure that the message is sent

        #print(f'Sent message: {message_value}')
        time.sleep(60)

except KeyboardInterrupt:
    pass

finally:
    producer.flush()  # Flush any remaining messages
    #producer.close()
