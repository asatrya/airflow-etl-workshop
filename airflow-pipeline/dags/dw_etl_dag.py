from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.sakila.jobs.generate_dim_date import run_job as run_generate_dim_date
from src.sakila.jobs.etl_dim_customer import run_job as run_etl_dim_customer
from src.sakila.jobs.etl_dim_movie import run_job as run_etl_dim_movie
from src.sakila.jobs.etl_dim_store import run_job as run_etl_dim_store
from src.sakila.jobs.etl_fact_sales import run_job as run_etl_fact_sales
import requests
import json
import os


# Define the default dag arguments.
default_args = {
    'owner': 'Aditya Satrya',
    'depends_on_past': False,
    'email': ['aditya.satrya@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


# Define the dag, the start date and how frequently it runs.
dag = DAG(
    dag_id='dw_etl_dag',
    default_args=default_args,
    start_date=datetime(2005, 5, 24, 0, 0),
    schedule_interval=timedelta(hours=24))


generate_dim_date = PythonOperator(
    task_id='generate_dim_date',
    provide_context=True,
    python_callable=run_generate_dim_date,
    dag=dag)

etl_dim_customer = PythonOperator(
    task_id='etl_dim_customer',
    provide_context=True,
    python_callable=run_etl_dim_customer,
    dag=dag)

etl_dim_movie = PythonOperator(
    task_id='etl_dim_movie',
    provide_context=True,
    python_callable=run_etl_dim_movie,
    dag=dag)

etl_dim_store = PythonOperator(
    task_id='etl_dim_store',
    provide_context=True,
    python_callable=run_etl_dim_store,
    dag=dag)

etl_fact_sales = PythonOperator(
    task_id='etl_fact_sales',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True,
    python_callable=run_etl_fact_sales,
    dag=dag)


# set dependency
generate_dim_date >> etl_fact_sales
etl_dim_customer >> etl_fact_sales
etl_dim_movie >> etl_fact_sales
etl_dim_store >> etl_fact_sales
