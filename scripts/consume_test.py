from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
    "stocks_raw",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms = 60000
)

print("Listening to topic stocks_raw...")
for msg in consumer:
    print(msg.value)
    