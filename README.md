# Data Pipeline for Exploring the Drug Publication

This personal project serves as a Data Engineering technical test for *Servier*

# 1. Pipeline Overview
Here is a high-level overview of the steps followed by the 'servier_pipeline' Airflow DAG:
1. **Ingestion and Preliminary Preprocessing**: Data is ingested from Minio (local S3), cleaned in .csv format, and then saved back in minio.
2. **Graph Calculation**: Cleaned data are merged resulting in the drug_graph_final.json file that can be found in the Minio bucket (impor/minio/airflow).

## Setup

### Requirements
    
    $ Ensure Docker is installed on your machine: 
    $ brew install --cask docker

### Clone project

    $ git clone https://github.com/SmadjaPaul/servier-de

### Start containers

Navigate to the project directory and run:

    $ docker-compose up

To run in the background:

    $ docker-compose up -d

### Check Access:

|        Application        |URL                          |Credentials                         |
|----------------|-------------------------------|-----------------------------|
|Airflow| [http://localhost:8080](http://localhost:8080) | ``` User: airflow``` <br> ``` Pass: airflow``` |         |
|MinIO| [http://localhost:9000](http://localhost:9000) | ``` User: admin``` <br> ``` Pass: minio-password``` |           |
|
  


Review the Makefile for additional commands. Note: Commands were tested on Linux.
```shell
echo "Setting Airflow settings"
echo "AIRFLOW_UID=$(id -u)" > .env
echo "Please make sure the user id is correct in .env"
nano .env

echo "Starting everything"
echo "Please wait a while or run 'make ps' to see if things are 'ready' and the init containers have exited (finished)"
echo "Airflow: http://127.0.0.1:8080 (Username: 'airflow', Password: 'airflow')"
echo "Minio: http://127.0.0.1:9002 (Username: 'admin', Password: 'minio-password')"
echo "Ctrl+c to exit when ready"
make up
echo "Cleaning up / removing everything"
make stop
```

### Test the Application:
If you want to run tests, use poetry after setting up a virtual environment and installing all dependencies. You can find the tests in test_func.py, including those related to the Bonus part.

    $ poetry run pytest

## How it works
1. Creates a `minio/minio` (MinIO) container
2. Use MC inside `minio/mc` (`minio-init`) to create a bucket and upload csv files
3. The `airflow-provision` creates a connector in Airflow that allows Airflow to access MinIO.
4. DAG to read files from MinIO, make data cleaning, and final calculation


## How to improve this project?
1. **Adding functionality**:
The final JSON file could be loaded into a Neo4J database, allowing for beautiful visual representations and enhanced analytics capabilities from tools like Jupyter Notebooks.

2. **DevOps**: Implement robust CI/CD and Infrastructure as Code (IaC) practices for a production environment.

3. **Alternative Workflow**: Consider using tools beyond pandas in a production context. Loading data directly into a SQL table and managing transformations with dbt (ELT) or using PySpark (ETL) are alternatives.

4. **Dynamic variable**: Transform all constants into environment variables and set up secrets for better credential management.

