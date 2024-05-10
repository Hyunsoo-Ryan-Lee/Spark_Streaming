from kafka import KafkaConsumer
from kafka import TopicPartition, OffsetAndMetadata
import json, ast
from datetime import datetime

topic_name = "dev"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=["localhost:9091", "localhost:9092", "localhost:9093"],
    # group_id="a",
    # auto_offset_reset="earliest",
    # enable_auto_commit=False,
)

while True:
    print(f"Topic {topic_name} Consumer Listening.....")
    for msg in consumer:
        output = {
            'timestamp' : datetime.utcfromtimestamp(msg.timestamp//1000).strftime('%Y-%m-%d %H:%M:%S'),
            'partition' : msg.partition,
            'offset' : msg.offset,
            'value' : ast.literal_eval(msg.value.decode('utf-8')),
        }
        print(output)