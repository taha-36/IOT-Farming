from confluent_kafka import Consumer
import json
import pandas as pd
import time
import os

KAFKA_NORTH_INDIA = "North-India"
KAFKA_SOUTH_INDIA = "South-India"
KAFKA_SOUTH_USA = "South-USA"
KAFKA_CENTRAL_USA = "Central-USA"
KAFKA_EAST_AFRICA = "East-Africa"
KAFKA_SERVER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")

consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "etl-group",
    "auto.offset.reset": "earliest"
})

def extract_north_india_data():
    consumer.subscribe([KAFKA_NORTH_INDIA])
    last_msg_time = time.time()
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.time() - last_msg_time > 10:
                break
            continue
        if msg.error():
            continue
        last_msg_time = time.time()
        message_value = json.loads(msg.value().decode("utf-8"))
        df = pd.DataFrame([message_value])
        path = "/opt/airflow/data/temp/north_india.csv"
        os.makedirs("temp", exist_ok=True)
        if os.path.exists(path):
            existing_df = pd.read_csv(path)
            pd.concat([existing_df, df], ignore_index=True).to_csv(path, index=False)
            print("Received From " + KAFKA_NORTH_INDIA)
        else:
            df.to_csv(path, index=False)
            print("Received From " + KAFKA_NORTH_INDIA)
        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_south_india_data():
    consumer.subscribe([KAFKA_SOUTH_INDIA])
    last_msg_time = time.time()
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.time() - last_msg_time > 10:
                break
            continue
        if msg.error():
            continue
        last_msg_time = time.time()
        message_value = json.loads(msg.value().decode("utf-8"))
        df = pd.DataFrame([message_value])
        path = "/opt/airflow/data/temp/south_india.csv"
        os.makedirs("/opt/airflow/data/temp", exist_ok=True)
        if os.path.exists(path):
            existing_df = pd.read_csv(path)
            pd.concat([existing_df, df], ignore_index=True).to_csv(path, index=False)
            print("Received From " + KAFKA_SOUTH_INDIA)
        else:
            df.to_csv(path, index=False)
            print("Received From " + KAFKA_SOUTH_INDIA)
        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_south_usa_data():
    consumer.subscribe([KAFKA_SOUTH_USA])
    last_msg_time = time.time()
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.time() - last_msg_time > 10:
                break
            continue
        if msg.error():
            continue
        last_msg_time = time.time()
        message_value = json.loads(msg.value().decode("utf-8"))
        df = pd.DataFrame([message_value])
        path = "/opt/airflow/data/temp/south_usa.csv"
        os.makedirs("/opt/airflow/data/temp", exist_ok=True)
        if os.path.exists(path):
            existing_df = pd.read_csv(path)
            pd.concat([existing_df, df], ignore_index=True).to_csv(path, index=False)
            print("Received From " + KAFKA_SOUTH_USA)
        else:
            df.to_csv(path, index=False)
            print("Received From " + KAFKA_SOUTH_USA)
        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_central_usa_data():
    consumer.subscribe([KAFKA_CENTRAL_USA])
    last_msg_time = time.time()
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.time() - last_msg_time > 10:
                break
            continue
        if msg.error():
            continue
        last_msg_time = time.time()
        message_value = json.loads(msg.value().decode("utf-8"))
        df = pd.DataFrame([message_value])
        path = "/opt/airflow/data/temp/central_usa.csv"
        os.makedirs("/opt/airflow/data/temp", exist_ok=True)
        if os.path.exists(path):
            existing_df = pd.read_csv(path)
            pd.concat([existing_df, df], ignore_index=True).to_csv(path, index=False)
            print("Received From " + KAFKA_CENTRAL_USA)
        else:
            df.to_csv(path, index=False)
            print("Received From " + KAFKA_CENTRAL_USA)
        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_east_africa_data():
    consumer.subscribe([KAFKA_EAST_AFRICA])
    last_msg_time = time.time()
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.time() - last_msg_time > 10:
                break
            continue
        if msg.error():
            continue
        last_msg_time = time.time()
        message_value = json.loads(msg.value().decode("utf-8"))
        df = pd.DataFrame([message_value])
        path = "/opt/airflow/data/temp/east_africa.csv"
        os.makedirs("/opt/airflow/data/temp", exist_ok=True)
        if os.path.exists(path):
            existing_df = pd.read_csv(path)
            pd.concat([existing_df, df], ignore_index=True).to_csv(path, index=False)
            print("Received From " + KAFKA_EAST_AFRICA)
        else:
            df.to_csv(path, index=False)
            print("Received From " + KAFKA_EAST_AFRICA)
        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_all_data():
    extract_north_india_data()
    extract_south_india_data()
    extract_south_usa_data()
    extract_central_usa_data()
    extract_east_africa_data()

extract_all_data()
