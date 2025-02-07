import pandas as pd
import pyodbc

# Connect to SQL Server
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=SalesDW;"
    "UID=sa;"
    "PWD=StrongP@ssword123;"
)
cursor = conn.cursor()

def load_table(csv_file, table_name):
    data = pd.read_csv(csv_file)
    for _, row in data.iterrows():
        columns = ", ".join(row.index)
        placeholders = ", ".join(["?" for _ in row.index])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, tuple(row))
    conn.commit()
    print(f"Data loaded into {table_name}.")

# Load data into staging tables
load_table("data/product_data.csv", "staging_products")
load_table("data/customer_data.csv", "staging_customers")
load_table("data/sales_data.csv", "staging_sales")

cursor.close()
conn.close()
