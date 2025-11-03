from azure.storage.blob import BlobServiceClient
import json
from io import StringIO
import pandas as pd
import time
import os

AZURE_CONNECTION_STRING = ""
CONTAINER_NAME = "iot-data"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

try:
    container_client.create_container()
except Exception:
    pass

def load_weather_data():
        blob_client = container_client.get_blob_client("weather_data.csv")
        if os.path.exists("temptrans/weather_data.csv"):
            df = pd.read_csv("temptrans/weather_data.csv")
            try:
                existing_data = blob_client.download_blob().readall().decode("utf-8")
                existing_df = pd.read_csv(StringIO(existing_data))
                updated_df = pd.concat([existing_df, df], ignore_index=True)
            except Exception:
                updated_df = df
            csv_buffer = StringIO()
            updated_df.to_csv(csv_buffer, index=False)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        else:
             return
        
def load_util_data():
        blob_client = container_client.get_blob_client("util_data.csv")
        if os.path.exists("temptrans/util_data.csv"):
            df = pd.read_csv("temptrans/util_data.csv")
            try:
                existing_data = blob_client.download_blob().readall().decode("utf-8")
                existing_df = pd.read_csv(StringIO(existing_data))
                updated_df = pd.concat([existing_df, df], ignore_index=True)
            except Exception:
                updated_df = df
            csv_buffer = StringIO()
            updated_df.to_csv(csv_buffer, index=False)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        else:
             return
        
def load_alerts_data():
        blob_client = container_client.get_blob_client("alerts_data.csv")
        if os.path.exists("temptrans/alerts_data.csv"):
            df = pd.read_csv("temptrans/alerts_data.csv")
            try:
                existing_data = blob_client.download_blob().readall().decode("utf-8")
                existing_df = pd.read_csv(StringIO(existing_data))
                updated_df = pd.concat([existing_df, df], ignore_index=True)
            except Exception:
                updated_df = df
            csv_buffer = StringIO()
            updated_df.to_csv(csv_buffer, index=False)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        else:
             return
        
load_weather_data()