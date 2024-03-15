from confluent_kafka import Consumer, KafkaError

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
        

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
