# Airflow ETL Workshop

## Purpose

This is a mini application to demonstrate ETL orchestration using Airflow. Because of the purpose, some parts are over engineered.

## Pre-requisite

This workshop assumes that:

1) You have a source database (sakila database) set up for you. 

   If that is not the case, you have to import schema and data for your source (OLTP) database by executing SQL queries in `sql/source_database/sakila_db_schema.sql` and `sql/source_database/sakila_db_data.sql`.

1) You have an empty schema for data warehouse set up for you. 

   If that is not the case, you have to setup table schema for your data warehouse (OLAP) by executing SQL queries in `sql/datawarehouse/sakila_dw_schema.sql`.

1) These tools are installed
   * Docker
   * Docker Compose

## How to Run

1) Clone this repository
   
   ```bash
   git clone https://github.com/asatrya/airflow-etl-workshop
   ```

   and go into project directory

   ```bash
   cd airflow-etl-workshop
   ```

1) Configure `.env`

   Edit and fill the variables using your source database and your data warehouse configurations.

   ```sh
   nano .env
   ```

1) Build and run Docker container

   ```sh
   docker-compose up -d
   ```

1) Access Airflow UI on <http://localhost:8080>

