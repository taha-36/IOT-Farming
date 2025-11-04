import pandas as pd
import os

def transform_data():
    if os.path.exists("/opt/airflow/data/temp/north_india.csv"):
        df = pd.read_csv("/opt/airflow/data/temp/north_india.csv")
        #Transformation pipeline

        df.to_csv("/opt/airflow/data/temptrans/north_india.csv", index=False)

    if os.path.exists("/opt/airflow/data/temp/south_india.csv"):
        df = pd.read_csv("/opt/airflow/data/temp/south_india.csv")
        #Transformation pipeline

        df.to_csv("/opt/airflow/data/temptrans/south_india.csv", index=False)

    if os.path.exists("/opt/airflow/data/temp/south_usa.csv"):
        df = pd.read_csv("/opt/airflow/data/temp/south_usa.csv")
        #Transformation pipeline

        df.to_csv("/opt/airflow/data/temptrans/south_usa.csv", index=False)

    if os.path.exists("/opt/airflow/data/temp/central_usa.csv"):
        df = pd.read_csv("/opt/airflow/data/temp/central_usa.csv")
        #Transformation pipeline

        df.to_csv("/opt/airflow/data/temptrans/central_usa.csv", index=False)

    if os.path.exists("/opt/airflow/data/temp/east_africa.csv"):
        df = pd.read_csv("/opt/airflow/data/temp/east_africa.csv")
        #Transformation pipeline

        df.to_csv("/opt/airflow/data/temptrans/east_africa.csv", index=False)

transform_data()