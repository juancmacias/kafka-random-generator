from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {'value': random.randint(0, 100)}
    producer.send('random-data', value=data)
    print(f"Sent: {data}")
    time.sleep(2)
