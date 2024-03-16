from confluent_kafka import Consumer, KafkaError
import urllib.parse as up
import psycopg2
import datetime

up.uses_netloc.append("postgres")

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
topic = 'quickstart-events'


consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0) 

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        print(msg.value().decode('utf-8'))
        decoded_data = msg.value().decode('utf-8')
        sql = f'INSERT into LZ_API_DATA(api_data, sys_insert_datetime) VALUES(\'{decoded_data}\', \'{datetime.datetime.now()}\')'
        cur.execute(sql)
        conn.commit()

except KeyboardInterrupt:
    pass

finally:
    conn.close()
    consumer.close()
