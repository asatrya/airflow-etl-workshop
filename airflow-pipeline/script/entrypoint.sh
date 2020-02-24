#!/usr/bin/env bash

# wait database server ready
printf "Waiting for database...\n\n"
sleep 10

# install custom DAGs' dependency
printf "Installing custom DAGs' dependencies...\n\n"
pip install -r dags/requirements.txt

# initialize the database
printf "Running initdb...\n\n"
airflow initdb

# set airflow variables
printf "Setting airflow variables...\n\n"
airflow variables --set SOURCE_MYSQL_HOST $SOURCE_MYSQL_HOST
airflow variables --set SOURCE_MYSQL_PORT $SOURCE_MYSQL_PORT
airflow variables --set SOURCE_MYSQL_USER $SOURCE_MYSQL_USER
airflow variables --set SOURCE_MYSQL_PASSWORD $SOURCE_MYSQL_PASSWORD
airflow variables --set SOURCE_MYSQL_ROOT_PASSWORD $SOURCE_MYSQL_ROOT_PASSWORD
airflow variables --set SOURCE_MYSQL_DATABASE $SOURCE_MYSQL_DATABASE

airflow variables --set DW_MYSQL_HOST $DW_MYSQL_HOST
airflow variables --set DW_MYSQL_PORT $DW_MYSQL_PORT
airflow variables --set DW_MYSQL_USER $DW_MYSQL_USER
airflow variables --set DW_MYSQL_PASSWORD $DW_MYSQL_PASSWORD
airflow variables --set DW_MYSQL_ROOT_PASSWORD $DW_MYSQL_ROOT_PASSWORD
airflow variables --set DW_MYSQL_DATABASE $DW_MYSQL_DATABASE

# start the scheduler and webserver
printf "Running scheduler and webserver...\n\n"
/usr/bin/supervisord