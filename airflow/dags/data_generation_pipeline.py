from datetime import datetime, timedelta
import os
import pandas as pd
import random
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator

# Initialize Faker
faker = Faker()

# Base directory where this script resides
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Resolve file paths dynamically
DATA_DIR = os.path.join(BASE_DIR, "../../data")
product_file = os.path.join(DATA_DIR, "product_data.csv")
customer_file = os.path.join(DATA_DIR, "customer_data.csv")
sales_file = os.path.join(DATA_DIR, "sales_data.csv")

# Function to get the starting ID
def get_starting_id(file_path, id_column):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        if not df.empty:
            return df[id_column].max() + 1  # Start from the next number
    return 1  # Default starting value if the file doesn't exist or is empty

# Function to append or create CSV files
def append_or_create_csv(data, file_path):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Append or create CSV
    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
        updated_data = pd.concat([existing_data, pd.DataFrame(data)], ignore_index=True)
        updated_data.to_csv(file_path, index=False)
    else:
        pd.DataFrame(data).to_csv(file_path, index=False)

    print(f"Data saved to {file_path}")

# Generate data and save to CSV
def generate_data():
    # Get starting IDs for each dataset
    start_product_id = get_starting_id(product_file, "product_id")
    start_customer_id = get_starting_id(customer_file, "customer_id") 
    start_transaction_id = get_starting_id(sales_file, "transaction_id")

    # Number of new records to generate
    num_products = 100
    num_customers = 200
    num_sales = 10000

    # Generate Products Data
    products = [
        {
            "product_id": i,
            "product_name": faker.word(),
            "category": faker.word(),
            "price": round(random.uniform(5, 100), 2),
        }
        for i in range(start_product_id, start_product_id + num_products)
    ]

    # Generate Customers Data
    customers = [
        {
            "customer_id": i,
            "name": faker.name(),
            "region": faker.state(),
            "signup_date": faker.date_between("-5y", "today"),
        }
        for i in range(start_customer_id, start_customer_id + num_customers)
    ]

    # Generate Sales Data
    sales = [
        {
            "transaction_id": i,
            "product_id": random.randint(start_product_id, start_product_id + num_products - 1),
            "customer_id": random.randint(start_customer_id, start_customer_id + num_customers - 1),
            "amount": round(random.uniform(10, 500), 2),
            "timestamp": faker.date_time_between("-5y", "now").strftime("%Y-%m-%d %H:%M:%S"),
        }
        for i in range(start_transaction_id, start_transaction_id + num_sales)
    ]

    # Append or create CSV files
    append_or_create_csv(products, product_file)
    append_or_create_csv(customers, customer_file)
    append_or_create_csv(sales, sales_file)

    print("Mock data generated and appended to the existing files.")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_generation_pipeline',
    default_args=default_args,
    description='Generate mock data for products, customers, and sales',
    schedule=None,  # Set to None for manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # PythonOperator to generate data
    generate_data_task = PythonOperator(
        task_id='generate_mock_data',
        python_callable=generate_data,
    )

    generate_data_task
