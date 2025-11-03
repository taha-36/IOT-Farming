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
KAFKA_SERVER = "localhost:9092"

consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "etl-group",
    "auto.offset.reset": "earliest"
})

def extract_north_india_data():
    consumer.subscribe([KAFKA_NORTH_INDIA])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        message_value = json.loads(msg.value().decode("utf-8"))
        print("Received:", message_value)

        df = pd.DataFrame([message_value])
        if os.path.exists("temp/north_india.csv"):
            existing_df = pd.read_csv("temp/north_india.csv")
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv("temp/north_india.csv", index=False)
        else:
            df.to_csv("temp/north_india.csv", index=False)

        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_south_india_data():
    consumer.subscribe([KAFKA_SOUTH_INDIA])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        message_value = json.loads(msg.value().decode("utf-8"))
        print("Received:", message_value)

        df = pd.DataFrame([message_value])
        if os.path.exists("temp/south_india.csv"):
            existing_df = pd.read_csv("temp/south_india.csv")
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv("temp/south_india.csv", index=False)
        else:
            df.to_csv("temp/south_india.csv", index=False)

        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_south_usa_data():
    consumer.subscribe([KAFKA_SOUTH_USA])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        message_value = json.loads(msg.value().decode("utf-8"))
        print("Received:", message_value)

        df = pd.DataFrame([message_value])
        if os.path.exists("temp/south_usa.csv"):
            existing_df = pd.read_csv("temp/south_usa.csv")
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv("temp/south_usa.csv", index=False)
        else:
            df.to_csv("temp/south_usa.csv", index=False)

        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_central_usa_data():
    consumer.subscribe([KAFKA_CENTRAL_USA])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        message_value = json.loads(msg.value().decode("utf-8"))
        print("Received:", message_value)

        df = pd.DataFrame([message_value])
        if os.path.exists("temp/central_usa.csv"):
            existing_df = pd.read_csv("temp/central_usa.csv")
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv("temp/central_usa.csv", index=False)
        else:
            df.to_csv("temp/central_usa.csv", index=False)

        consumer.commit(asynchronous=False)
        time.sleep(1)

def extract_east_africa_data():
    consumer.subscribe([KAFKA_EAST_AFRICA])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        message_value = json.loads(msg.value().decode("utf-8"))
        print("Received:", message_value)

        df = pd.DataFrame([message_value])
        if os.path.exists("temp/east_africa.csv"):
            existing_df = pd.read_csv("temp/east_africa.csv")
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv("temp/east_africa.csv", index=False)
        else:
            df.to_csv("temp/east_africa.csv", index=False)

        consumer.commit(asynchronous=False)
        time.sleep(1)
