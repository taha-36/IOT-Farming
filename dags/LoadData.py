from azure.storage.blob import BlobServiceClient
import json
from io import StringIO
import pandas as pd
import time
import os

BLOB_NORTH_INDIA = "north_india"
BLOB_SOUTH_INDIA = "south_india"
BLOB_SOUTH_USA = "south_usa"
BLOB_CENTRAL_USA = "central_usa"
BLOB_EAST_AFRICA = "east_africa"

AZURE_CONNECTION_STRING = ""
CONTAINER_NAME = "iot-data"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

try:
    container_client.create_container()
except Exception:
    pass

def load_data():
    load_to_server(BLOB_NORTH_INDIA)
    load_to_server(BLOB_SOUTH_INDIA)
    load_to_server(BLOB_SOUTH_USA)
    load_to_server(BLOB_CENTRAL_USA)
    load_to_server(BLOB_EAST_AFRICA)
        
def load_to_server(blobName):
    blob_client = container_client.get_blob_client(blobName + ".csv")
    if os.path.exists("temptrans/" + blobName + ".csv"):
        df = pd.read_csv("temptrans/" + blobName + ".csv")
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
    
load_data()
    