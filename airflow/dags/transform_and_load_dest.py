from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from helpers import test_odbc_connection, execute_sql  # Importing from helpers.py

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG using "with" statement
with DAG(
    dag_id='transform_and_load_dest',
    default_args=default_args,
    description='A DAG for testing and loading SalesDW tables',
    schedule=None,
    catchup=False,
) as dag:

    # Test ODBC Connection Task
    test_connection_task = PythonOperator(
        task_id='test_odbc_connection',
        python_callable=test_odbc_connection,
    )

    # Deduplicate staging tables
    deduplicate_products = PythonOperator(
        task_id='deduplicate_staging_products',
        python_callable=execute_sql,
        op_args=["""
        WITH Deduplicated AS (
            SELECT product_id, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS row_num
            FROM staging_products
        )
        DELETE FROM staging_products WHERE product_id IN (
            SELECT product_id FROM Deduplicated WHERE row_num > 1
        );
        """],
    )
    
    deduplicate_customers = PythonOperator(
        task_id='deduplicate_staging_customers',
        python_callable=execute_sql,
        op_args=["""
        WITH Deduplicated AS (
            SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS row_num
            FROM staging_customers
        )
        DELETE FROM staging_customers WHERE customer_id IN (
            SELECT customer_id FROM Deduplicated WHERE row_num > 1
        );
        """],
    )
    
    deduplicate_sales = PythonOperator(
        task_id='deduplicate_staging_sales',
        python_callable=execute_sql,
        op_args=["""
        WITH Deduplicated AS (
            SELECT transaction_id, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS row_num
            FROM staging_sales
        )
        DELETE FROM staging_sales WHERE transaction_id IN (
            SELECT transaction_id FROM Deduplicated WHERE row_num > 1
        );
        """],
    )
    
    # Load Data into DimProducts
    load_dim_products = PythonOperator(
        task_id='load_dim_products',
        python_callable=execute_sql,
        op_args=["""
        MERGE DimProducts AS target
        USING staging_products AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET target.product_name = source.product_name, target.category = source.category, target.price = source.price
        WHEN NOT MATCHED THEN
            INSERT (product_id, product_name, category, price)
            VALUES (source.product_id, source.product_name, source.category, source.price);
        """],
    )
    
    # Load Data into DimCustomers
    load_dim_customers = PythonOperator(
        task_id='load_dim_customers',
        python_callable=execute_sql,
        op_args=["""
        MERGE DimCustomers AS target
        USING staging_customers AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET target.name = source.name, target.region = source.region, target.signup_date = source.signup_date
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, region, signup_date, valid_from, valid_to)
            VALUES (source.customer_id, source.name, source.region, source.signup_date, GETDATE(), NULL);
        """],
    )
    
    # Load Data into FactSales
    load_fact_sales = PythonOperator(
        task_id='load_fact_sales',
        python_callable=execute_sql,
        op_args=["""
        INSERT INTO FactSales (transaction_id, product_id, customer_id, amount, timestamp)
        SELECT staging_sales.transaction_id, staging_sales.product_id, staging_sales.customer_id, staging_sales.amount, staging_sales.timestamp
        FROM staging_sales
        WHERE NOT EXISTS (
            SELECT 1 FROM FactSales WHERE FactSales.transaction_id = staging_sales.transaction_id
        );
        """],
    )

    # Define task dependencies
    test_connection_task >> [deduplicate_products, deduplicate_customers, deduplicate_sales]
    deduplicate_products >> load_dim_products
    deduplicate_customers >> load_dim_customers
    deduplicate_sales >> load_fact_sales
