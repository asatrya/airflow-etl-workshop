# Airflow ETL Workshop

## Purpose

This is a mini application to demonstrate ETL orchestration using Airflow. Because of the purpose, some parts are over engineered.

## How to Run

1) Prerequisite
   * Docker installed
   * Docker Compose installed

1) Configure `.env`

   ```sh
   nano .env
   ```

1) Build and run Docker container

   ```sh
   docker-compose up -d
   ```

1) Access Airflow UI on <http://localhost:8080>

### Local Development Setup

For local development purpose, you can setup local Airflow with other dependencies installed in your machine by following these steps:

1. Set Airflow Home Directory

   ```sh
   cd airflow-pipeline/
   export AIRFLOW_HOME=$PWD
   ```

1. Set `FERNET_KEY`

   ```sh
   export FERNET_KEY=Zp3FDPCdBoq03Begm2RJOL_3obEwrleTdkS02UNgV48=
   ```

1. Change `airflow.cfg`

   For local develpoment, it's enough to use SequentialExecutor, so change corresponding line to be:

   ```txt
   ...
   executor = SequentialExecutor
   ...
   sql_alchemy_conn = sqlite:///$AIRFLOW_HOME/airflow.db
   ...
   ```

1. Setup and activate virtual environment

   ```sh
   virtualenv --no-site-packages venv
   source venv/bin/activate
   ```

1. Set Airflow Variables

   ```sh
   airflow variables --set SOURCE_MYSQL_HOST 35.240.238.96
   airflow variables --set SOURCE_MYSQL_PORT 3306
   airflow variables --set SOURCE_MYSQL_USER training
   airflow variables --set SOURCE_MYSQL_PASSWORD training
   airflow variables --set SOURCE_MYSQL_ROOT_PASSWORD training
   airflow variables --set SOURCE_MYSQL_DATABASE sakila

   airflow variables --set DW_MYSQL_HOST 35.240.238.96
   airflow variables --set DW_MYSQL_PORT 3306
   airflow variables --set DW_MYSQL_USER training
   airflow variables --set DW_MYSQL_PASSWORD training
   airflow variables --set DW_MYSQL_ROOT_PASSWORD training
   airflow variables --set DW_MYSQL_DATABASE sakila_dw
   ```

1. Install Airflow and Python dependencies

   ```sh
   sudo apt install libpq-dev python-dev
   pip install -r dags/requirements.txt
   ```

1. Initialize Airflow database

   ```sh
   airflow initdb
   ```

1. Validate Airflow installation

   ```sh
   airflow version
   ```

1. Run Airflow scheduler and webserver

   ```sh
   airflow scheduler -D
   airflow webserver -D
   ```
