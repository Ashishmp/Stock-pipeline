# scripts/publish_test_message.py
from kafka import KafkaProducer
import json, time

p = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
p.send('stocks_raw', {'symbol':'TEST','timestamp':int(time.time()),'raw':{'price':'100'}})
p.flush()
print('Sent test message')
