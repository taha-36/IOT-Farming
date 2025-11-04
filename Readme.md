# IoT Farming

This project implements an **IoT-based farming data ETL pipeline** using **Apache Airflow**, **Kafka**, and **PostgreSQL**. The pipeline ingests simulated farm sensor data, processes it, and stores it for analysis. It is fully containerized using Docker and Docker Compose.

---

## Dataset

The project uses a CSV dataset (`source.csv`) representing farm sensor data. Each record includes:

- **region** – Geographic region (North India, South India, South USA, Central USA, East Africa)  
- **crop_type** – Type of crop (Wheat, Soybean, Maize, Rice, Cotton)  
- **soil_moisture_%** – Soil moisture percentage  
- **soil_pH** – Soil pH value  
- **temperature_C** – Temperature in Celsius  
- **rainfall_mm** – Rainfall in millimeters  
- **humidity_%** – Humidity percentage  
- **sunlight_hours** – Hours of sunlight  
- **irrigation_type** – Type of irrigation (Sprinkler, Drip, Manual, None)  
- **fertilizer_type** – Type of fertilizer (Organic, Inorganic, Mixed)  
- **pesticide_usage_ml** – Pesticide usage in milliliters  
- **sowing_date / harvest_date** – Crop sowing and harvest dates  
- **total_days** – Days between sowing and harvest  
- **yield_kg_per_hectare** – Crop yield  
- **latitude / longitude** – Farm coordinates  
- **NDVI_index** – Vegetation index  
- **crop_disease_status** – Crop disease level (None, Mild, Moderate, Severe)  

## Features

1. **Data Generation** – Simulated IoT sensor data is sampled from the source CSV and sent to Kafka topics based on the region.  
2. **Kafka Integration** – Each region has its own Kafka topic for ingesting messages.  
3. **Airflow ETL DAGs** – Airflow DAGs orchestrate the pipeline, including:  
   - **Data Generation** – Sends data to Kafka for a set duration (configurable).  
   - **Data Extraction** – Consumes Kafka messages, converts them to DataFrames, and saves them as CSV files (`temp` folder).  
   - **Data Transformation** – Processes raw data and stores it in a structured format (`temptrans` folder).  
   - **Data Loading** – Can be extended to store transformed data in databases like PostgreSQL or Azure Blob Storage.  
4. **Dockerized Environment** – No local installations required; all dependencies run inside containers.  

---

## Folder Structure

project-root/
│
├─ dags/ # Airflow DAG scripts
│ ├─ DataGenerator.py
│ ├─ ExtractData.py
│ ├─ TransformData.py
│ ├─ LoadData.py
│ └─ Pipeline.py
│
├─ data/ # Source CSV file
│ └─ source.csv
│
├─ temp/ # Generated raw CSVs by DAGs (created at runtime)
├─ temptrans/ # Transformed CSVs (created at runtime)
│
├─ logs/ # Airflow logs
├─ plugins/ # Airflow plugins (optional)
├─ docker-compose.yml # Docker Compose setup
└─ README.md

> **Note:** `temp` and `temptrans` are created dynamically by Airflow tasks. `source.csv` is stored in the `data` folder, which is mounted in Airflow containers.

---

## Prerequisites

- Docker  
- Docker Compose  

No other installations are required. All Python dependencies (`kafka-python`, `confluent-kafka`, `azure-storage-blob`) are installed inside Airflow containers via `docker-compose.yml`.

---

## Setup Instructions
Start the environment:

use the cmd "docker-compose up --build -d" in the project folder
This will start:

   Zookeeper & Kafka broker

   Kafka initialization (topics for each region)

   PostgreSQL database for Airflow metadata

   Airflow webserver & scheduler (with dependencies installed)

Access Airflow Web UI:

URL: http://localhost:8080

Username: airflow

Password: airflow

Trigger the etl_pipeline DAG manually or schedule it.

How It Works
Data Generation Task – Reads source.csv, samples records, and sends them to Kafka topics for 60 seconds (configurable).

Extraction Task – Consumes messages from Kafka topics and writes them to CSV files under temp/.

Transformation Task – Reads raw CSVs from temp/, processes them, and saves cleaned data to temptrans/.

Load Task – Can insert transformed data into databases or cloud storage.