import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.models import Variable
from helpers import load_table  # Importing from helpers.py

# Dynamically resolve data directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = Variable.get("DATA_DIR", default_var=os.path.join(BASE_DIR, "../../data"))

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'transform_and_load_staging',
    default_args=default_args,
    description='DAG to load CSV data into SQL Server staging tables',
    schedule=None,
    catchup=False,
) as dag:

    # Task to load products
    load_products = PythonOperator(
        task_id='load_staging_products',
        python_callable=load_table,
        op_args=[os.path.join(DATA_DIR, "product_data.csv"), "staging_products", "product_id"],
    )

    # Task to load customers
    load_customers = PythonOperator(
        task_id='load_staging_customers',
        python_callable=load_table,
        op_args=[os.path.join(DATA_DIR, "customer_data.csv"), "staging_customers", "customer_id"],
    )

    # Task to load sales
    load_sales = PythonOperator(
        task_id='load_staging_sales',
        python_callable=load_table,
        op_args=[os.path.join(DATA_DIR, "sales_data.csv"), "staging_sales", "transaction_id"],
    )

    # Define task dependencies (run in parallel)
    [load_products, load_customers, load_sales]
