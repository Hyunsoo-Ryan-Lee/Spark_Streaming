import time, json
from json import dumps
from kafka import KafkaProducer
from pprint import pprint
from fakedata import create_fakeuser


topic_name = "user"
producer = KafkaProducer(
    bootstrap_servers=["localhost:9091", "localhost:9092", "localhost:9093"],
    # value_serializer=lambda x: dumps(x).encode("utf-8"),
)
ORDER_LIMIT = 100
for i in range(1, ORDER_LIMIT+1):
    data = create_fakeuser()
    # data = {
    #     "order_id": i,
    #     "user_id": f"hyunsoo_{i}",
    #     "total_cost": i*1000
    # }

    producer.send(topic_name, json.dumps(data).encode("utf-8"))
    print("=="*30)
    print(data)
    print(f">>>>>>>>>>>  {i} MESSAGE SENT  <<<<<<<<<<<<")
    time.sleep(1)