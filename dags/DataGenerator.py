from kafka import KafkaProducer
import json
import random
import time
import pandas as pd
import os

KAFKA_NORTH_INDIA = "North-India"
KAFKA_SOUTH_INDIA = "South-India"
KAFKA_SOUTH_USA = "South-USA"
KAFKA_CENTRAL_USA = "Central-USA"
KAFKA_EAST_AFRICA = "East-Africa"
KAFKA_SERVER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

dataSource = pd.read_csv("/opt/airflow/data/source.csv")

def generate_data():
    sample = dataSource.sample()
    data = sample.drop(['farm_id', 'sensor_id', 'timestamp'] , axis=1)
    return data.to_dict(orient='records')[0]
def delay():
    return time.sleep(5)

start_time = time.time()
duration = 10
while time.time() - start_time < duration:
    record = generate_data()
    selector = record['region']
    match selector:
        case "North India":
            producer.send(KAFKA_NORTH_INDIA, value=record)
            print("Sent Record to " + selector)
        case "South USA":
            producer.send(KAFKA_SOUTH_USA, value=record)
            print("Sent Record to " + selector)
        case "South India":
            producer.send(KAFKA_SOUTH_INDIA, value=record)
            print("Sent Record to " + selector)
        case "Central USA":
            producer.send(KAFKA_CENTRAL_USA, value=record)
            print("Sent Record to " + selector)
        case "East Africa":
            producer.send(KAFKA_EAST_AFRICA, value=record)
            print("Sent Record to " + selector)
    delay()