import time, json
from json import dumps
from kafka import KafkaProducer

topic_name = "dev"
producer = KafkaProducer(
    bootstrap_servers=["localhost:9091", "localhost:9092", "localhost:9093"],
    # value_serializer=lambda x: dumps(x).encode("utf-8"),
)
ORDER_LIMIT = 100
for i in range(1, ORDER_LIMIT+1):
    data = {
        "order_id": i,
        "user_id": f"hyunsoo_{i}",
        "total_cost": i*1000
    }

    producer.send(topic_name, json.dumps(data).encode("utf-8"))
    print(f"Done Sending..{i}")
    time.sleep(1)